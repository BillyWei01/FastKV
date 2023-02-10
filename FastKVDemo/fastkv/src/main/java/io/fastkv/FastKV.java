package io.fastkv;

import androidx.annotation.NonNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import io.fastkv.Container.*;

@SuppressWarnings("rawtypes")
public final class FastKV extends AbsFastKV {
    private FileChannel aChannel;
    private FileChannel bChannel;
    private MappedByteBuffer aBuffer;
    private MappedByteBuffer bBuffer;

    private int updateStart;
    private int updateSize;
    private int removeStart;
    private boolean sizeChanged;

    private final List<String> deletedFiles = new ArrayList<>();

    // The default writing mode is non-blocking (write partial data with mmap).
    // If mmap API throw IOException, degrade to blocking mode (write all data to disk with blocking I/O).
    // User could assign to using blocking mode by FastKV.Builder
    static final int NON_BLOCKING = 0;
    static final int ASYNC_BLOCKING = 1;
    static final int SYNC_BLOCKING = 2;
    private int writingMode;

    // Only take effect when mode is not NON_BLOCKING
    private boolean autoCommit = true;

    private final Executor applyExecutor = new LimitExecutor();
    private final TagExecutor externalExecutor = new TagExecutor();

    FastKV(final String path, final String name, Encoder[] encoders, int writingMode) {
        super(path, name, encoders);
        this.writingMode = writingMode;

        synchronized (data) {
            FastKVConfig.getExecutor().execute(this::loadData);
            while (!startLoading) {
                try {
                    // wait util loadData() get the object lock
                    data.wait();
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    private synchronized void loadData() {
        // we got the object lock, notify the waiter to continue the constructor
        synchronized (data) {
            startLoading = true;
            data.notify();
        }
        long start = System.nanoTime();
        if (!loadFromCFile() && writingMode == NON_BLOCKING) {
            loadFromABFile();
        }
        if (fastBuffer == null) {
            fastBuffer = new FastBuffer(PAGE_SIZE);
        }
        if (logger != null) {
            long t = (System.nanoTime() - start) / 1000000;
            info("loading finish, data len:" + dataEnd + ", get keys:" + data.size() + ", use time:" + t + " ms");
        }
    }

    private void loadFromABFile() {
        File aFile = new File(path, name + A_SUFFIX);
        File bFile = new File(path, name + B_SUFFIX);
        try {
            if (!Util.makeFileIfNotExist(aFile) || !Util.makeFileIfNotExist(bFile)) {
                error(new Exception(OPEN_FILE_FAILED));
                toBlockingMode();
                return;
            }
            RandomAccessFile aAccessFile = new RandomAccessFile(aFile, "rw");
            RandomAccessFile bAccessFile = new RandomAccessFile(bFile, "rw");
            long aFileLen = aAccessFile.length();
            long bFileLen = bAccessFile.length();
            aChannel = aAccessFile.getChannel();
            bChannel = bAccessFile.getChannel();
            try {
                aBuffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, aFileLen > 0 ? aFileLen : PAGE_SIZE);
                aBuffer.order(ByteOrder.LITTLE_ENDIAN);
                bBuffer = bChannel.map(FileChannel.MapMode.READ_WRITE, 0, bFileLen > 0 ? bFileLen : PAGE_SIZE);
                bBuffer.order(ByteOrder.LITTLE_ENDIAN);
            } catch (IOException e) {
                error(e);
                toBlockingMode();
                tryBlockingIO(aFile, bFile);
                return;
            }
            fastBuffer = new FastBuffer(aBuffer.capacity());

            if (aFileLen == 0 && bFileLen == 0) {
                dataEnd = DATA_START;
            } else {
                int aDataSize = aBuffer.getInt();
                long aCheckSum = aBuffer.getLong();
                int bDataSize = bBuffer.getInt();
                long bCheckSum = bBuffer.getLong();

                boolean isAValid = false;
                if (aDataSize >= 0 && (aDataSize <= aFileLen - DATA_START)) {
                    dataEnd = DATA_START + aDataSize;
                    aBuffer.rewind();
                    aBuffer.get(fastBuffer.hb, 0, dataEnd);
                    if (aCheckSum == fastBuffer.getChecksum(DATA_START, aDataSize) && parseData() == 0) {
                        checksum = aCheckSum;
                        isAValid = true;
                    }
                }
                if (isAValid) {
                    if (aFileLen != bFileLen || !isABFileEqual()) {
                        warning(new Exception("B file error"));
                        copyBuffer(aBuffer, bBuffer, dataEnd);
                    }
                } else {
                    boolean isBValid = false;
                    if (bDataSize >= 0 && (bDataSize <= bFileLen - DATA_START)) {
                        data.clear();
                        clearInvalid();
                        dataEnd = DATA_START + bDataSize;
                        if (fastBuffer.hb.length != bBuffer.capacity()) {
                            fastBuffer = new FastBuffer(bBuffer.capacity());
                        }
                        bBuffer.rewind();
                        bBuffer.get(fastBuffer.hb, 0, dataEnd);
                        if (bCheckSum == fastBuffer.getChecksum(DATA_START, bDataSize) && parseData() == 0) {
                            warning(new Exception("A file error"));
                            copyBuffer(bBuffer, aBuffer, dataEnd);
                            checksum = bCheckSum;
                            isBValid = true;
                        }
                    }
                    if (!isBValid) {
                        error(BOTH_FILES_ERROR);
                        clearData();
                    }
                }
            }
        } catch (Exception e) {
            error(e);
            resetMemory();
            toBlockingMode();
        }
    }

    private boolean isABFileEqual() {
        FastBuffer tempBuffer = new FastBuffer(dataEnd);
        bBuffer.rewind();
        bBuffer.get(tempBuffer.hb, 0, dataEnd);
        byte[] a = fastBuffer.hb;
        byte[] b = tempBuffer.hb;
        for (int i = 0; i < dataEnd; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    private boolean loadFromCFile() {
        boolean hadWriteToABFile = false;
        File cFile = new File(path, name + C_SUFFIX);
        File tmpFile = new File(path, name + TEMP_SUFFIX);
        try {
            File srcFile = null;
            if (cFile.exists()) {
                srcFile = cFile;
            } else if (tmpFile.exists()) {
                srcFile = tmpFile;
            }
            if (srcFile != null) {
                if (loadWithBlockingIO(srcFile)) {
                    if (writingMode == NON_BLOCKING) {
                        if (writeToABFile(fastBuffer)) {
                            info("recover from c file");
                            hadWriteToABFile = true;
                            deleteCFiles();
                        } else {
                            writingMode = ASYNC_BLOCKING;
                        }
                    }
                } else {
                    resetMemory();
                    deleteCFiles();
                }
            } else {
                // Handle the case:
                // User opening with non-blocking mode at first, and change to blocking mode in later.
                if (writingMode != NON_BLOCKING) {
                    File aFile = new File(path, name + A_SUFFIX);
                    File bFile = new File(path, name + B_SUFFIX);
                    if (aFile.exists() && bFile.exists()) {
                        tryBlockingIO(aFile, bFile);
                    }
                }
            }
        } catch (Exception e) {
            error(e);
        }
        return hadWriteToABFile;
    }

    private boolean writeToABFile(FastBuffer buffer) {
        int fileLen = buffer.hb.length;
        File aFile = new File(path, name + A_SUFFIX);
        File bFile = new File(path, name + B_SUFFIX);
        try {
            if (!Util.makeFileIfNotExist(aFile) || !Util.makeFileIfNotExist(bFile)) {
                throw new Exception(OPEN_FILE_FAILED);
            }
            RandomAccessFile aAccessFile = new RandomAccessFile(aFile, "rw");
            RandomAccessFile bAccessFile = new RandomAccessFile(bFile, "rw");
            aAccessFile.setLength(fileLen);
            bAccessFile.setLength(fileLen);
            aChannel = aAccessFile.getChannel();
            bChannel = bAccessFile.getChannel();
            aBuffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            aBuffer.order(ByteOrder.LITTLE_ENDIAN);
            bBuffer = bChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            bBuffer.order(ByteOrder.LITTLE_ENDIAN);
            aBuffer.put(buffer.hb, 0, dataEnd);
            bBuffer.put(buffer.hb, 0, dataEnd);
            return true;
        } catch (Exception e) {
            error(e);
        }
        return false;
    }

    private void copyBuffer(MappedByteBuffer src, MappedByteBuffer des, int end) {
        if (src.capacity() != des.capacity()) {
            try {
                FileChannel channel = (des == bBuffer) ? bChannel : aChannel;
                MappedByteBuffer newBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, src.capacity());
                newBuffer.order(ByteOrder.LITTLE_ENDIAN);
                if (des == bBuffer) {
                    bBuffer = newBuffer;
                } else {
                    aBuffer = newBuffer;
                }
                des = newBuffer;
            } catch (IOException e) {
                error(e);
                toBlockingMode();
                return;
            }
        }
        src.rewind();
        des.rewind();
        src.limit(end);
        des.put(src);
        src.limit(src.capacity());
    }

    public synchronized void putBoolean(String key, boolean value) {
        checkKey(key);
        BooleanContainer c = (BooleanContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.BOOLEAN);
            int offset = fastBuffer.position;
            fastBuffer.put((byte) (value ? 1 : 0));
            updateChange();
            data.put(key, new BooleanContainer(offset, value));
            checkIfCommit();
        } else if (c.value != value) {
            c.value = value;
            updateBoolean((byte) (value ? 1 : 0), c.offset);
            checkIfCommit();
        }
    }

    public synchronized void putInt(String key, int value) {
        checkKey(key);
        IntContainer c = (IntContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.INT);
            int offset = fastBuffer.position;
            fastBuffer.putInt(value);
            updateChange();
            data.put(key, new IntContainer(offset, value));
            checkIfCommit();
        } else if (c.value != value) {
            long sum = (value ^ c.value) & 0xFFFFFFFFL;
            c.value = value;
            updateInt32(value, sum, c.offset);
            checkIfCommit();
        }
    }

    public synchronized void putFloat(String key, float value) {
        checkKey(key);
        FloatContainer c = (FloatContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.FLOAT);
            int offset = fastBuffer.position;
            fastBuffer.putInt(Float.floatToRawIntBits(value));
            updateChange();
            data.put(key, new FloatContainer(offset, value));
            checkIfCommit();
        } else if (c.value != value) {
            int newValue = Float.floatToRawIntBits(value);
            long sum = (Float.floatToRawIntBits(c.value) ^ newValue) & 0xFFFFFFFFL;
            c.value = value;
            updateInt32(newValue, sum, c.offset);
            checkIfCommit();
        }
    }

    public synchronized void putLong(String key, long value) {
        checkKey(key);
        LongContainer c = (LongContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.LONG);
            int offset = fastBuffer.position;
            fastBuffer.putLong(value);
            updateChange();
            data.put(key, new LongContainer(offset, value));
            checkIfCommit();
        } else if (c.value != value) {
            long sum = value ^ c.value;
            c.value = value;
            updateInt64(value, sum, c.offset);
            checkIfCommit();
        }
    }

    public synchronized void putDouble(String key, double value) {
        checkKey(key);
        DoubleContainer c = (DoubleContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.DOUBLE);
            int offset = fastBuffer.position;
            fastBuffer.putLong(Double.doubleToRawLongBits(value));
            updateChange();
            data.put(key, new DoubleContainer(offset, value));
            checkIfCommit();
        } else if (c.value != value) {
            long newValue = Double.doubleToRawLongBits(value);
            long sum = Double.doubleToRawLongBits(c.value) ^ newValue;
            c.value = value;
            updateInt64(newValue, sum, c.offset);
            checkIfCommit();
        }
    }

    public synchronized void putString(String key, String value) {
        checkKey(key);
        if (value == null) {
            remove(key);
        } else {
            StringContainer c = (StringContainer) data.get(key);
            if (value.length() * 3 < INTERNAL_LIMIT) {
                // putString is a frequent operation,
                // so we make some redundant code to speed up putString method.
                fastPutString(key, value, c);
            } else {
                byte[] bytes = value.isEmpty() ? EMPTY_ARRAY : value.getBytes(StandardCharsets.UTF_8);
                addOrUpdate(key, value, bytes, c, DataType.STRING);
            }
        }
    }

    public synchronized void putArray(String key, byte[] value) {
        checkKey(key);
        if (value == null) {
            remove(key);
        } else {
            ArrayContainer c = (ArrayContainer) data.get(key);
            addOrUpdate(key, value, value, c, DataType.ARRAY);
        }
    }

    /**
     * @param key     The name of the data to modify
     * @param value   The new value
     * @param encoder The encoder to encode value to byte[], encoder must register in  Builder.encoder(),
     *                for decoding byte[] to object in next loading.
     * @param <T>     Type of value
     */
    public synchronized <T> void putObject(String key, T value, Encoder<T> encoder) {
        checkKey(key);
        if (encoder == null) {
            throw new IllegalArgumentException("Encoder is null");
        }
        String tag = encoder.tag();
        if (tag == null || tag.isEmpty() || tag.length() > 50) {
            throw new IllegalArgumentException("Invalid encoder tag:" + tag);
        }
        if (!encoderMap.containsKey(tag)) {
            throw new IllegalArgumentException("Encoder hasn't been registered");
        }

        if (value == null) {
            remove(key);
            return;
        }
        byte[] obj = null;
        try {
            obj = encoder.encode(value);
        } catch (Exception e) {
            error(e);
        }
        if (obj == null) {
            remove(key);
            return;
        }

        // assemble object bytes
        int tagSize = FastBuffer.getStringSize(tag);
        FastBuffer buffer = new FastBuffer(1 + tagSize + obj.length);
        buffer.put((byte) tagSize);
        buffer.putString(tag);
        buffer.putBytes(obj);
        byte[] bytes = buffer.hb;

        ObjectContainer c = (ObjectContainer) data.get(key);
        addOrUpdate(key, value, bytes, c, DataType.OBJECT);
    }

    public synchronized void putStringSet(String key, Set<String> set) {
        if (set == null) {
            remove(key);
        } else {
            putObject(key, set, StringSetEncoder.INSTANCE);
        }
    }

    public synchronized void remove(String key) {
        BaseContainer container = data.get(key);
        if (container != null) {
            final String oldFileName;
            data.remove(key);
            bigValueCache.remove(key);
            byte type = container.getType();
            if (type <= DataType.DOUBLE) {
                int keySize = FastBuffer.getStringSize(key);
                int start = container.offset - (2 + keySize);
                remove(type, start, container.offset + TYPE_SIZE[type]);
                oldFileName = null;
            } else {
                VarContainer c = (VarContainer) container;
                remove(type, c.start, c.offset + c.valueSize);
                oldFileName = c.external ? (String) c.value : null;
            }
            byte newByte = (byte) (type | DataType.DELETE_MASK);
            if (writingMode == NON_BLOCKING) {
                aBuffer.putLong(4, checksum);
                aBuffer.put(removeStart, newByte);
                bBuffer.putLong(4, checksum);
                bBuffer.put(removeStart, newByte);
            } else {
                fastBuffer.putLong(4, checksum);
            }
            removeStart = 0;
            if (oldFileName != null) {
                if (writingMode == NON_BLOCKING) {
                    FastKVConfig.getExecutor().execute(() -> Util.deleteFile(new File(path + name, oldFileName)));
                } else {
                    deletedFiles.add(oldFileName);
                }
            }
            checkGC();
            checkIfCommit();
        }
    }

    public synchronized void clear() {
        clearData();
        if (writingMode != NON_BLOCKING) {
            deleteCFiles();
        }
    }

    public void putAll(Map<String, Object> values) {
        putAll(values, null);
    }

    /**
     * Batch put objects.
     * Only support type in [boolean, int, long, float, double, String, byte[], Set of String] and object with encoder.
     *
     * @param values   map of key to value
     * @param encoders map of value Class to Encoder
     */
    public synchronized void putAll(Map<String, Object> values, Map<Class, Encoder> encoders) {
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key != null && !key.isEmpty()) {
                if (value instanceof String) {
                    putString(key, (String) value);
                } else if (value instanceof Boolean) {
                    putBoolean(key, (Boolean) value);
                } else if (value instanceof Integer) {
                    putInt(key, (Integer) value);
                } else if (value instanceof Long) {
                    putLong(key, (Long) value);
                } else if (value instanceof Float) {
                    putFloat(key, (Float) value);
                } else if (value instanceof Double) {
                    putDouble(key, (Double) value);
                } else if (value instanceof Set) {
                    Set set = (Set) value;
                    if (!set.isEmpty() && set.iterator().next() instanceof String) {
                        //noinspection unchecked
                        putStringSet(key, (Set<String>) value);
                    }
                } else if (value instanceof byte[]) {
                    putArray(key, (byte[]) value);
                } else {
                    if (encoders != null) {
                        Encoder encoder = encoders.get(value.getClass());
                        if (encoder != null) {
                            //noinspection unchecked
                            putObject(key, value, encoder);
                        } else {
                            warning(new Exception("missing encoder for type:" + value.getClass()));
                        }
                    } else {
                        warning(new Exception("missing encoders"));
                    }
                }
            }
        }
    }

    /**
     * Forces any changes to be written to the storage device containing the mapped file.
     * No need to call this unless what's had written is very import.
     * The system crash or power off before data syncing to disk might make recently update loss.
     */
    public synchronized void force() {
        if (writingMode == NON_BLOCKING) {
            aBuffer.force();
            bBuffer.force();
        }
    }

    /**
     * When you open file with mode of SYNC_BLOCKING or ASYNC_BLOCKING,
     * It will auto commit after every putting or removing, by default.
     * If you need to batch update several key-values, you could call this method at first,
     * and call {@link #commit()} after updating, that method will recover {@link #autoCommit} to 'true' again.
     */
    public synchronized void disableAutoCommit() {
        this.autoCommit = false;
    }

    public synchronized boolean commit() {
        autoCommit = true;
        return commitToCFile();
    }

    private void checkIfCommit() {
        if (writingMode != NON_BLOCKING && autoCommit) {
            commitToCFile();
        }
    }

    private boolean commitToCFile() {
        if (writingMode == ASYNC_BLOCKING) {
            applyExecutor.execute(this::writeToCFile);
        } else if (writingMode == SYNC_BLOCKING) {
            return writeToCFile();
        }
        return true;
    }

    private synchronized boolean writeToCFile() {
        try {
            File tmpFile = new File(path, name + TEMP_SUFFIX);
            if (Util.makeFileIfNotExist(tmpFile)) {
                RandomAccessFile accessFile = new RandomAccessFile(tmpFile, "rw");
                accessFile.setLength(dataEnd);
                accessFile.write(fastBuffer.hb, 0, dataEnd);
                accessFile.close();
                File cFile = new File(path, name + C_SUFFIX);
                if (!cFile.exists() || cFile.delete()) {
                    if (tmpFile.renameTo(cFile)) {
                        clearDeletedFiles();
                        return true;
                    } else {
                        warning(new Exception("rename failed"));
                    }
                }
            }
        } catch (Exception e) {
            error(e);
        }
        return false;
    }

    private void clearDeletedFiles() {
        if (!deletedFiles.isEmpty()) {
            for (String oldFileName : deletedFiles) {
                FastKVConfig.getExecutor().execute(() -> Util.deleteFile(new File(path + name, oldFileName)));
            }
            deletedFiles.clear();
        }
    }

    private void toBlockingMode() {
        writingMode = ASYNC_BLOCKING;
        Util.closeQuietly(aChannel);
        Util.closeQuietly(bChannel);
        aChannel = null;
        bChannel = null;
        aBuffer = null;
        bBuffer = null;
    }

    private void clearData() {
        if (writingMode == NON_BLOCKING) {
            try {
                resetBuffer(aBuffer);
                resetBuffer(bBuffer);
            } catch (IOException e) {
                toBlockingMode();
            }
        }
        resetMemory();
        Util.deleteFile(new File(path + name));
    }

    private void resetBuffer(MappedByteBuffer buffer) throws IOException {
        if (buffer.capacity() != PAGE_SIZE) {
            FileChannel channel = buffer == aBuffer ? aChannel : bChannel;
            channel.truncate(PAGE_SIZE);
            MappedByteBuffer newBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
            newBuffer.order(ByteOrder.LITTLE_ENDIAN);
            if (buffer == aBuffer) {
                aBuffer = newBuffer;
            } else {
                bBuffer = newBuffer;
            }
            buffer = newBuffer;
        }
        buffer.putInt(0, 0);
        buffer.putLong(4, 0L);
    }


    private void wrapHeader(String key, byte type) {
        wrapHeader(key, type, TYPE_SIZE[type]);
    }

    private void wrapHeader(String key, byte type, int valueSize) {
        int keySize = FastBuffer.getStringSize(key);
        checkKeySize(keySize);
        updateSize = 2 + keySize + valueSize;
        preparePutBytes();
        fastBuffer.put(type);
        putKey(key, keySize);
    }

    private void updateChange() {
        checksum ^= fastBuffer.getChecksum(updateStart, updateSize);
        if (writingMode == NON_BLOCKING) {
            // When size of changed data is more than 8 bytes,
            // checksum might fail to check the integrity in small probability.
            // So we make the dataLen to be negative,
            // if crash happen when writing data to mmap memory,
            // we can know that the writing had not accomplished.
            aBuffer.putInt(0, -1);
            syncABBuffer(aBuffer);
            aBuffer.putInt(0, dataEnd - DATA_START);

            // bBuffer doesn't need to mark dataLen's part before writing bytes,
            // cause aBuffer has already written completely.
            // We just need to have one file to be integrated at least at any time.
            syncABBuffer(bBuffer);
        } else {
            if (sizeChanged) {
                fastBuffer.putInt(0, dataEnd - DATA_START);
            }
            fastBuffer.putLong(4, checksum);
        }
        sizeChanged = false;
        removeStart = 0;
        updateSize = 0;
    }

    private void syncABBuffer(MappedByteBuffer buffer) {
        if (sizeChanged && buffer != aBuffer) {
            buffer.putInt(0, dataEnd - DATA_START);
        }
        buffer.putLong(4, checksum);
        if (removeStart != 0) {
            buffer.put(removeStart, fastBuffer.hb[removeStart]);
        }
        if (updateSize != 0) {
            buffer.position(updateStart);
            buffer.put(fastBuffer.hb, updateStart, updateSize);
        }
    }

    private void ensureSize(int allocate) {
        int capacity = fastBuffer.hb.length;
        int expected = dataEnd + allocate;
        if (expected >= capacity) {
            if (invalidBytes > allocate && invalidBytes > bytesThreshold()) {
                gc(allocate);
            } else {
                int newCapacity = getNewCapacity(capacity, expected);
                byte[] bytes = new byte[newCapacity];
                System.arraycopy(fastBuffer.hb, 0, bytes, 0, dataEnd);
                fastBuffer.hb = bytes;
                if (writingMode == NON_BLOCKING) {
                    try {
                        aBuffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
                        aBuffer.order(ByteOrder.LITTLE_ENDIAN);
                        bBuffer = bChannel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
                        bBuffer.order(ByteOrder.LITTLE_ENDIAN);
                    } catch (IOException e) {
                        error(new Exception(MAP_FAILED, e));
                        fastBuffer.putInt(0, dataEnd - DATA_START);
                        fastBuffer.putLong(4, checksum);
                        toBlockingMode();
                    }
                }
            }
        }
    }

    private void updateBoolean(byte value, int offset) {
        checksum ^= shiftCheckSum(1L, offset);
        if (writingMode == NON_BLOCKING) {
            aBuffer.putLong(4, checksum);
            aBuffer.put(offset, value);
            bBuffer.putLong(4, checksum);
            bBuffer.put(offset, value);
        } else {
            fastBuffer.putLong(4, checksum);
        }
        fastBuffer.hb[offset] = value;
    }

    private void updateInt32(int value, long sum, int offset) {
        checksum ^= shiftCheckSum(sum, offset);
        if (writingMode == NON_BLOCKING) {
            aBuffer.putLong(4, checksum);
            aBuffer.putInt(offset, value);
            bBuffer.putLong(4, checksum);
            bBuffer.putInt(offset, value);
        } else {
            fastBuffer.putLong(4, checksum);
        }
        fastBuffer.putInt(offset, value);
    }

    private void updateInt64(long value, long sum, int offset) {
        checksum ^= shiftCheckSum(sum, offset);
        if (writingMode == NON_BLOCKING) {
            aBuffer.putLong(4, checksum);
            aBuffer.putLong(offset, value);
            bBuffer.putLong(4, checksum);
            bBuffer.putLong(offset, value);
        } else {
            fastBuffer.putLong(4, checksum);
        }
        fastBuffer.putLong(offset, value);
    }

    private void updateBytes(int offset, byte[] bytes) {
        int size = bytes.length;
        checksum ^= fastBuffer.getChecksum(offset, size);
        fastBuffer.position = offset;
        fastBuffer.putBytes(bytes);
        checksum ^= fastBuffer.getChecksum(offset, size);
        if (writingMode == NON_BLOCKING) {
            aBuffer.putInt(0, -1);
            aBuffer.putLong(4, checksum);
            aBuffer.position(offset);
            aBuffer.put(bytes);
            aBuffer.putInt(0, dataEnd - DATA_START);
            bBuffer.putLong(4, checksum);
            bBuffer.position(offset);
            bBuffer.put(bytes);
        } else {
            fastBuffer.putLong(4, checksum);
        }
    }

    private void preparePutBytes() {
        ensureSize(updateSize);
        updateStart = dataEnd;
        dataEnd += updateSize;
        fastBuffer.position = updateStart;
        sizeChanged = true;
    }

    private void putKey(String key, int keySize) {
        fastBuffer.put((byte) keySize);
        if (keySize == key.length()) {
            //noinspection deprecation
            key.getBytes(0, keySize, fastBuffer.hb, fastBuffer.position);
            fastBuffer.position += keySize;
        } else {
            fastBuffer.putString(key);
        }
    }

    private void putStringValue(String value, int valueSize) {
        fastBuffer.putShort((short) valueSize);
        if (valueSize == value.length()) {
            //noinspection deprecation
            value.getBytes(0, valueSize, fastBuffer.hb, fastBuffer.position);
        } else {
            fastBuffer.putString(value);
        }
    }

    private void fastPutString(String key, String value, StringContainer c) {
        int stringLen = FastBuffer.getStringSize(value);
        if (c == null) {
            int keyLen = FastBuffer.getStringSize(key);
            checkKeySize(keyLen);
            // 4 bytes = type:1, keyLen: 1, stringLen:2
            // preSize include size of [type|keyLen|key|stringLen], which is "4+lengthOf(key)"
            int preSize = 4 + keyLen;
            updateSize = preSize + stringLen;
            preparePutBytes();
            fastBuffer.put(DataType.STRING);
            putKey(key, keyLen);
            putStringValue(value, stringLen);
            data.put(key, new StringContainer(updateStart, updateStart + preSize, value, stringLen, false));
            updateChange();
        } else {
            final String oldFileName;
            boolean needCheckGC = false;
            // preSize: bytes count from start to value offset
            int preSize = c.offset - c.start;
            if (c.valueSize == stringLen) {
                checksum ^= fastBuffer.getChecksum(c.offset, c.valueSize);
                if (stringLen == value.length()) {
                    //noinspection deprecation
                    value.getBytes(0, stringLen, fastBuffer.hb, c.offset);
                } else {
                    fastBuffer.position = c.offset;
                    fastBuffer.putString(value);
                }
                updateStart = c.offset;
                updateSize = stringLen;
                oldFileName = null;
            } else {
                updateSize = preSize + stringLen;
                preparePutBytes();
                fastBuffer.put(DataType.STRING);
                int keyBytes = preSize - 3;
                System.arraycopy(fastBuffer.hb, c.start + 1, fastBuffer.hb, fastBuffer.position, keyBytes);
                fastBuffer.position += keyBytes;
                putStringValue(value, stringLen);

                remove(DataType.STRING, c.start, c.offset + c.valueSize);
                needCheckGC = true;
                oldFileName = c.external ? (String) c.value : null;

                c.external = false;
                c.start = updateStart;
                c.offset = updateStart + preSize;
                c.valueSize = stringLen;
            }
            c.value = value;
            updateChange();
            if (needCheckGC) {
                checkGC();
            }
            if (oldFileName != null) {
                if (writingMode == NON_BLOCKING) {
                    FastKVConfig.getExecutor().execute(() -> Util.deleteFile(new File(path + name, oldFileName)));
                } else {
                    deletedFiles.add(oldFileName);
                }
            }
        }
        checkIfCommit();
    }

    private void addOrUpdate(String key, Object value, byte[] bytes, VarContainer c, byte type) {
        if (c == null) {
            addObject(key, value, bytes, type);
        } else {
            if (!c.external && c.valueSize == bytes.length) {
                updateBytes(c.offset, bytes);
                c.value = value;
            } else {
                updateObject(key, value, bytes, c);
            }
        }
        checkIfCommit();
    }

    private void addObject(String key, Object value, byte[] bytes, byte type) {
        int offset = saveArray(key, bytes, type);
        if (offset != 0) {
            int size;
            Object v;
            boolean external = tempExternalName != null;
            if (external) {
                bigValueCache.put(key, value);
                size = Util.NAME_SIZE;
                v = tempExternalName;
                tempExternalName = null;
            } else {
                size = bytes.length;
                v = value;
            }
            BaseContainer c;
            if (type == DataType.STRING) {
                c = new StringContainer(updateStart, offset, (String) v, size, external);
            } else if (type == DataType.ARRAY) {
                c = new ArrayContainer(updateStart, offset, v, size, external);
            } else {
                c = new ObjectContainer(updateStart, offset, v, size, external);
            }
            data.put(key, c);
            updateChange();
        }
    }

    private void updateObject(String key, Object value, byte[] bytes, VarContainer c) {
        int offset = saveArray(key, bytes, c.getType());
        if (offset != 0) {
            String oldFileName = c.external ? (String) c.value : null;
            remove(c.getType(), c.start, c.offset + c.valueSize);
            boolean external = tempExternalName != null;
            c.start = updateStart;
            c.offset = offset;
            c.external = external;
            if (external) {
                bigValueCache.put(key, value);
                c.value = tempExternalName;
                c.valueSize = Util.NAME_SIZE;
                tempExternalName = null;
            } else {
                c.value = value;
                c.valueSize = bytes.length;
            }
            updateChange();
            checkGC();
            if (oldFileName != null) {
                if (writingMode == NON_BLOCKING) {
                    FastKVConfig.getExecutor().execute(() -> Util.deleteFile(new File(path + name, oldFileName)));
                } else {
                    deletedFiles.add(oldFileName);
                }
            }
        }
    }

    private int saveArray(String key, byte[] value, byte type) {
        tempExternalName = null;
        if (value.length < INTERNAL_LIMIT) {
            return wrapArray(key, value, type);
        } else {
            info("large value, key: " + key + ", size: " + value.length);
            String fileName = Util.randomName();

            // The reference of 'value' will not be gc before 'saveBytes' finish,
            // So before the value saving to disk, it could be read with 'externalCache'.
            externalCache.put(fileName, value);
            externalExecutor.execute(key, () -> Util.saveBytes(new File(path + name, fileName), value));

            tempExternalName = fileName;
            byte[] fileNameBytes = new byte[Util.NAME_SIZE];
            //noinspection deprecation
            fileName.getBytes(0, Util.NAME_SIZE, fileNameBytes, 0);
            return wrapArray(key, fileNameBytes, (byte) (type | DataType.EXTERNAL_MASK));
        }
    }

    private int wrapArray(String key, byte[] value, byte type) {
        wrapHeader(key, type, 2 + value.length);
        fastBuffer.putShort((short) value.length);
        int offset = fastBuffer.position;
        fastBuffer.putBytes(value);
        return offset;
    }

    private void remove(byte type, int start, int end) {
        countInvalid(start, end);
        byte newByte = (byte) (type | DataType.DELETE_MASK);
        byte oldByte = fastBuffer.hb[start];
        int shift = (start & 7) << 3;
        checksum ^= ((long) (newByte ^ oldByte) & 0xFF) << shift;
        fastBuffer.hb[start] = newByte;
        removeStart = start;
    }

    private void checkGC() {
        if (invalidBytes >= (bytesThreshold() << 1)
                || invalids.size() >= (dataEnd < (1 << 14) ? BASE_GC_KEYS_THRESHOLD : BASE_GC_KEYS_THRESHOLD << 1)) {
            gc(0);
        }
    }

    void gc(int allocate) {
        Collections.sort(invalids);
        mergeInvalids();

        final Segment head = invalids.get(0);
        final int gcStart = head.start;
        final int newDataEnd = dataEnd - invalidBytes;
        final int newDataSize = newDataEnd - DATA_START;
        final int updateSize = newDataEnd - gcStart;
        final int gcSize = dataEnd - gcStart;
        final boolean fullChecksum = newDataSize < gcSize + updateSize;
        if (!fullChecksum) {
            checksum ^= fastBuffer.getChecksum(gcStart, gcSize);
        }
        // compact and record shift
        int n = invalids.size();
        final int remain = dataEnd - invalids.get(n - 1).end;
        int shiftCount = (remain > 0) ? n : n - 1;
        int[] srcToShift = new int[shiftCount << 1];
        int desPos = head.start;
        int srcPos = head.end;
        for (int i = 1; i < n; i++) {
            Segment q = invalids.get(i);
            int size = q.start - srcPos;
            System.arraycopy(fastBuffer.hb, srcPos, fastBuffer.hb, desPos, size);
            int index = (i - 1) << 1;
            srcToShift[index] = srcPos;
            srcToShift[index + 1] = srcPos - desPos;
            desPos += size;
            srcPos = q.end;
        }
        if (remain > 0) {
            System.arraycopy(fastBuffer.hb, srcPos, fastBuffer.hb, desPos, remain);
            int index = (n - 1) << 1;
            srcToShift[index] = srcPos;
            srcToShift[index + 1] = srcPos - desPos;
        }
        clearInvalid();

        if (fullChecksum) {
            checksum = fastBuffer.getChecksum(DATA_START, newDataEnd - DATA_START);
        } else {
            checksum ^= fastBuffer.getChecksum(gcStart, newDataEnd - gcStart);
        }
        dataEnd = newDataEnd;

        if (writingMode == NON_BLOCKING) {
            aBuffer.putInt(0, -1);
            aBuffer.putLong(4, checksum);
            aBuffer.position(gcStart);
            aBuffer.put(fastBuffer.hb, gcStart, updateSize);
            aBuffer.putInt(0, newDataSize);
            bBuffer.putInt(0, newDataSize);
            bBuffer.putLong(4, checksum);
            bBuffer.position(gcStart);
            bBuffer.put(fastBuffer.hb, gcStart, updateSize);
        } else {
            fastBuffer.putInt(0, newDataSize);
            fastBuffer.putLong(4, checksum);
        }

        updateOffset(gcStart, srcToShift);
        int expectedEnd = newDataEnd + allocate;
        if (fastBuffer.hb.length - expectedEnd > TRUNCATE_THRESHOLD) {
            truncate(expectedEnd);
        }
        info(GC_FINISH);
    }

    private void truncate(int expectedEnd) {
        // reserve at least one page space
        int newCapacity = getNewCapacity(PAGE_SIZE, expectedEnd + PAGE_SIZE);
        if (newCapacity >= fastBuffer.hb.length) {
            return;
        }
        byte[] bytes = new byte[newCapacity];
        System.arraycopy(fastBuffer.hb, 0, bytes, 0, dataEnd);
        fastBuffer.hb = bytes;
        if (writingMode == NON_BLOCKING) {
            try {
                aChannel.truncate(newCapacity);
                aBuffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
                aBuffer.order(ByteOrder.LITTLE_ENDIAN);
                bChannel.truncate(newCapacity);
                bBuffer = bChannel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
                bBuffer.order(ByteOrder.LITTLE_ENDIAN);
            } catch (IOException e) {
                error(new Exception(MAP_FAILED, e));
                toBlockingMode();
            }
        }
        info(TRUNCATE_FINISH);
    }

    public interface Logger {
        void i(@NonNull String name, @NonNull String message);

        void w(@NonNull String name, @NonNull Exception e);

        void e(@NonNull String name, @NonNull Exception e);
    }

    public interface Encoder<T> {
        String tag();

        byte[] encode(@NonNull T obj);

        T decode(@NonNull byte[] bytes, int offset, int length);
    }

    public static class Builder {
        private static final Map<String, FastKV> INSTANCE_MAP = new ConcurrentHashMap<>();
        private final String path;
        private final String name;
        private Encoder[] encoders;
        private int writingMode = NON_BLOCKING;

        public Builder(String path, String name) {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("path is empty");
            }
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name is empty");
            }
            this.path = path.endsWith("/") ? path : (path + '/');
            this.name = name;
        }

        /**
         * Set obj Encoders
         *
         * @param encoders The encoder array to decode the bytes to obj.
         * @return the builder
         */
        public Builder encoder(Encoder[] encoders) {
            this.encoders = encoders;
            return this;
        }

        /**
         * Assigned writing mode to SYNC_BLOCKING.
         * <p>
         * In non-blocking mode (write data with mmap),
         * it might loss update if the system crash or power off before flush data to disk.
         * You could use {@link #force()} to avoid loss update, or use SYNC_BLOCKING mode.
         * <p>
         * In blocking mode, every update will write all data to the file, which is expensive cost.
         * <p>
         * So it's recommended to use blocking mode only if the data is every important.
         * <p>
         *
         * @return the builder
         */
        public Builder blocking() {
            writingMode = SYNC_BLOCKING;
            return this;
        }

        /**
         * Similar to {@link #blocking()}, but put writing task to async thread.
         *
         * @return the builder
         */
        public Builder asyncBlocking() {
            writingMode = ASYNC_BLOCKING;
            return this;
        }

        public FastKV build() {
            String key = path + name;
            FastKV kv = INSTANCE_MAP.get(key);
            if (kv == null) {
                synchronized (Builder.class) {
                    kv = INSTANCE_MAP.get(key);
                    if (kv == null) {
                        kv = new FastKV(path, name, encoders, writingMode);
                        INSTANCE_MAP.put(key, kv);
                    }
                }
            }
            return kv;
        }
    }

    @Override
    public synchronized @NonNull
    String toString() {
        return "FastKV: path:" + path + " name:" + name;
    }
}
