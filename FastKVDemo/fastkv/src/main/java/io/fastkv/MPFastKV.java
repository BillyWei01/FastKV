package io.fastkv;

import android.content.SharedPreferences;
import android.os.FileObserver;
import android.os.Handler;
import android.os.Looper;
import android.os.Message;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import io.fastkv.FastKV.*;
import io.fastkv.Container.*;

/**
 * Multiprocess FastKV <br>
 * <p>
 * This class support cross process storage and update notify.
 * It implements API of SharePreferences, so it could be used as SharePreferences.<br>
 * <p>
 * Note:
 * To support cross process storage, MPFastKV needs to do many state checks, it's slower than {@link FastKV}.
 * So if you don't need to access the data in multiprocess, just use FastKV.
 */
@SuppressWarnings("rawtypes")
public final class MPFastKV extends AbsFastKV implements SharedPreferences, SharedPreferences.Editor {
    private static final int MSG_REFRESH = 1;
    private static final int MSG_APPLY = 2;
    private static final int MSG_DATA_CHANGE = 3;
    private static final int MSG_CLEAR = 4;
    private static final int LOCK_TIMEOUT = 3000;

    private static final Random random = new Random();

    private final File aFile;
    private final File bFile;
    private RandomAccessFile aAccessFile;
    private RandomAccessFile bAccessFile;
    private FileChannel aChannel;
    private FileChannel bChannel;
    private MappedByteBuffer aBuffer;
    private FileLock bFileLock;

    private int[] updateStartAndSize = new int[16];
    private int updateCount = 0;
    private int updateStart;
    private int updateSize;
    private final List<String> deletedFiles = new ArrayList<>();

    private long updateHash;
    private boolean needFullWrite = false;

    private final Executor applyExecutor = new LimitExecutor();
    private final Executor refreshExecutor = new LimitExecutor();

    // We need to keep reference to the observer in case of gc recycle it.
    @SuppressWarnings("FieldCanBeLocal")
    private final KVFileObserver fileObserver;
    private final Set<String> changedKey = new HashSet<>();
    private final ArrayList<SharedPreferences.OnSharedPreferenceChangeListener> listeners = new ArrayList<>();

    MPFastKV(final String path, final String name, Encoder[] encoders, boolean needWatchFileChange) {
        super(path, name, encoders);
        aFile = new File(path, name + A_SUFFIX);
        bFile = new File(path, name + B_SUFFIX);

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

        if (needWatchFileChange) {
            fileObserver = new KVFileObserver(bFile.getPath());
            fileObserver.startWatching();
        } else {
            fileObserver = null;
        }
    }

    private synchronized void loadData() {
        // we got the object lock, notify the waiter to continue the constructor
        synchronized (data) {
            startLoading = true;
            data.notify();
        }
        long start = System.nanoTime();
        if (!loadFromCFile()) {
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
        try {
            if (!Util.makeFileIfNotExist(aFile) || !Util.makeFileIfNotExist(bFile)) {
                error(new Exception(OPEN_FILE_FAILED));
                return;
            }
            aAccessFile = new RandomAccessFile(aFile, "rw");
            bAccessFile = new RandomAccessFile(bFile, "rw");
            long aFileLen = aAccessFile.length();
            long bFileLen = bAccessFile.length();
            aChannel = aAccessFile.getChannel();
            bChannel = bAccessFile.getChannel();
            FileLock lock = bChannel.lock();
            try {
                try {
                    aBuffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, aFileLen > 0 ? aFileLen : PAGE_SIZE);
                    aBuffer.order(ByteOrder.LITTLE_ENDIAN);
                } catch (IOException e) {
                    error(e);
                    tryBlockingIO(aFile, bFile);
                    return;
                }

                if (aFileLen == 0 && bFileLen == 0) {
                    dataEnd = DATA_START;
                    bAccessFile.setLength(PAGE_SIZE);
                    bChannel.truncate(PAGE_SIZE);
                } else {
                    if (loadWithBlockingIO(bFile)) {
                        boolean isAEqualB = false;
                        if (aFileLen == bFileLen && fastBuffer.hb.length == aBuffer.capacity()) {
                            byte[] b = fastBuffer.hb;
                            byte[] a = new byte[dataEnd];
                            aBuffer.get(a, 0, dataEnd);
                            int i = 0;
                            for (; i < dataEnd; i++) {
                                if (a[i] != b[i]) break;
                            }
                            isAEqualB = i == dataEnd;
                        }
                        if (!isAEqualB) {
                            warning(new Exception("A file error"));
                            fullWriteBufferToA();
                        }
                    } else {
                        updateCount = 0;
                        resetData();
                        if (fastBuffer == null || fastBuffer.hb.length != aBuffer.capacity()) {
                            fastBuffer = new FastBuffer(aBuffer.capacity());
                        }
                        int aDataSize = aBuffer.getInt();
                        boolean isAValid = false;
                        if (aDataSize >= 0 && (aDataSize <= aFileLen - DATA_START)) {
                            dataEnd = DATA_START + aDataSize;
                            long aCheckSum = aBuffer.getLong(4);
                            aBuffer.rewind();
                            aBuffer.get(fastBuffer.hb, 0, dataEnd);
                            if (aCheckSum == fastBuffer.getChecksum(DATA_START, aDataSize) && parseData() == 0) {
                                checksum = aCheckSum;
                                isAValid = true;
                            }
                        }
                        if (isAValid) {
                            warning(new Exception("B file error"));
                            fullWriteAToB();
                        } else {
                            error(BOTH_FILES_ERROR);
                            clearData();
                        }
                    }
                }
                getUpdateHash();
            } finally {
                lock.release();
            }
        } catch (Exception e) {
            error(e);
            resetMemory();
        }
    }

    private void getUpdateHash() {
        if (aBuffer != null && dataEnd + 8 < aBuffer.capacity()) {
            updateHash = aBuffer.getLong(dataEnd);
        }
    }

    private void fullWriteAToB() {
        try {
            if (!Util.makeFileIfNotExist(bFile)) {
                return;
            }
            setBFileSize(aBuffer.capacity());
            syncAToB(0, dataEnd);
        } catch (Exception e) {
            error(e);
        }
    }

    private void fullWriteBufferToA() {
        try {
            if (alignAToBuffer()) {
                aBuffer.position(0);
                aBuffer.put(fastBuffer.hb, 0, dataEnd);
            }
        } catch (Exception e) {
            error(e);
        }
    }

    private boolean alignAToBuffer() {
        int bufferSize = fastBuffer.hb.length;
        try {
            if (aAccessFile == null) {
                if (!Util.makeFileIfNotExist(aFile)) {
                    return false;
                }
                aAccessFile = new RandomAccessFile(aFile, "rw");
            }
            if (aAccessFile.length() != bufferSize) {
                aAccessFile.setLength(bufferSize);
            }
            if (aChannel == null) {
                aChannel = aAccessFile.getChannel();
            } else if (aChannel.size() != bufferSize) {
                aChannel.truncate(bufferSize);
            }
            if (aBuffer == null || aBuffer.capacity() != bufferSize) {
                aBuffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
                aBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
        } catch (Throwable e) {
            error(e);
            return false;
        }
        return true;
    }

    // MPFastKV do not write data to CFile,
    // just in case of user write data with FastKV at first and then change to MPFastKV
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
                    if (writeToABFile(fastBuffer)) {
                        info("recover from c file");
                        hadWriteToABFile = true;
                    }
                } else {
                    resetMemory();
                }
                deleteCFiles();
            }
        } catch (Exception e) {
            error(e);
        }
        return hadWriteToABFile;
    }

    private boolean writeToABFile(FastBuffer buffer) {
        int bufferLen = buffer.hb.length;
        try {
            if (!Util.makeFileIfNotExist(aFile) || !Util.makeFileIfNotExist(bFile)) {
                throw new Exception(OPEN_FILE_FAILED);
            }
            if (bAccessFile == null) {
                bAccessFile = new RandomAccessFile(bFile, "rw");
            }
            if (bChannel == null) {
                bChannel = bAccessFile.getChannel();
            }
            FileLock lock = bFileLock == null ? bChannel.lock() : null;
            try {
                alignAToBuffer();
                aBuffer.put(buffer.hb, 0, dataEnd);
                getUpdateHash();
                if (bAccessFile.length() != bufferLen) {
                    bAccessFile.setLength(bufferLen);
                }
                bChannel.truncate(bufferLen);
                syncAToB(0, dataEnd);
                bChannel.force(false);
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
            return true;
        } catch (Exception e) {
            error(e);
        }
        return false;
    }

    private void syncAToB(int offset, int length) throws IOException {
        MappedByteBuffer buffer = aBuffer;
        buffer.position(offset);
        buffer.limit(offset + length);
        if (bChannel.size() != buffer.capacity()) {
            bChannel.truncate(buffer.capacity());
        }
        bChannel.position(offset);
        while (buffer.hasRemaining()) {
            bChannel.write(buffer);
        }
        buffer.limit(buffer.capacity());
    }

    private void syncBufferToA(int offset, int length) {
        byte[] bytes = fastBuffer.hb;
        aBuffer.position(offset);
        aBuffer.put(bytes, offset, length);
    }

    @Nullable
    @Override
    public Set<String> getStringSet(String key, @Nullable Set<String> defValues) {
        Set<String> set = getStringSet(key);
        return set != null ? set : defValues;
    }

    @Override
    public Editor edit() {
        return this;
    }

    private void notifyListeners(ArrayList<SharedPreferences.OnSharedPreferenceChangeListener> listeners, String key) {
        for (SharedPreferences.OnSharedPreferenceChangeListener listener : listeners) {
            listener.onSharedPreferenceChanged(this, key);
        }
    }

    @Override
    public synchronized void registerOnSharedPreferenceChangeListener(OnSharedPreferenceChangeListener listener) {
        if (listener == null) {
            return;
        }
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    @Override
    public synchronized void unregisterOnSharedPreferenceChangeListener(OnSharedPreferenceChangeListener listener) {
        listeners.remove(listener);
    }

    @Override
    public synchronized Editor putBoolean(String key, boolean value) {
        checkKey(key);
        lockAndCheckUpdate();
        BooleanContainer c = (BooleanContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.BOOLEAN);
            int offset = fastBuffer.position;
            fastBuffer.put((byte) (value ? 1 : 0));
            updateChange();
            data.put(key, new BooleanContainer(offset, value));
            markChanged(key);
        } else if (c.value != value) {
            c.value = value;
            updateBoolean((byte) (value ? 1 : 0), c.offset);
            markChanged(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putInt(String key, int value) {
        checkKey(key);
        lockAndCheckUpdate();
        IntContainer c = (IntContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.INT);
            int offset = fastBuffer.position;
            fastBuffer.putInt(value);
            updateChange();
            data.put(key, new IntContainer(offset, value));
            markChanged(key);
        } else if (c.value != value) {
            long sum = (value ^ c.value) & 0xFFFFFFFFL;
            c.value = value;
            updateInt32(value, sum, c.offset);
            markChanged(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putFloat(String key, float value) {
        checkKey(key);
        lockAndCheckUpdate();
        FloatContainer c = (FloatContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.FLOAT);
            int offset = fastBuffer.position;
            fastBuffer.putInt(Float.floatToRawIntBits(value));
            updateChange();
            data.put(key, new FloatContainer(offset, value));
            markChanged(key);
        } else if (c.value != value) {
            int newValue = Float.floatToRawIntBits(value);
            long sum = (Float.floatToRawIntBits(c.value) ^ newValue) & 0xFFFFFFFFL;
            c.value = value;
            updateInt32(newValue, sum, c.offset);
            markChanged(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putLong(String key, long value) {
        checkKey(key);
        lockAndCheckUpdate();
        LongContainer c = (LongContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.LONG);
            int offset = fastBuffer.position;
            fastBuffer.putLong(value);
            updateChange();
            data.put(key, new LongContainer(offset, value));
            markChanged(key);
        } else if (c.value != value) {
            long sum = value ^ c.value;
            c.value = value;
            updateInt64(value, sum, c.offset);
            markChanged(key);
        }
        return this;
    }

    public synchronized void putDouble(String key, double value) {
        checkKey(key);
        lockAndCheckUpdate();
        DoubleContainer c = (DoubleContainer) data.get(key);
        if (c == null) {
            wrapHeader(key, DataType.DOUBLE);
            int offset = fastBuffer.position;
            fastBuffer.putLong(Double.doubleToRawLongBits(value));
            updateChange();
            data.put(key, new DoubleContainer(offset, value));
            markChanged(key);
        } else if (c.value != value) {
            long newValue = Double.doubleToRawLongBits(value);
            long sum = Double.doubleToRawLongBits(c.value) ^ newValue;
            c.value = value;
            updateInt64(newValue, sum, c.offset);
            markChanged(key);
        }
    }

    @Override
    public synchronized Editor putString(String key, String value) {
        checkKey(key);
        if (value == null) {
            remove(key);
        } else {
            lockAndCheckUpdate();
            markChanged(key);
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
        return this;
    }

    public synchronized void putArray(String key, byte[] value) {
        checkKey(key);
        if (value == null) {
            remove(key);
        } else {
            lockAndCheckUpdate();
            markChanged(key);
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

        lockAndCheckUpdate();
        markChanged(key);
        ObjectContainer c = (ObjectContainer) data.get(key);
        addOrUpdate(key, value, bytes, c, DataType.OBJECT);
    }

    public synchronized Editor putStringSet(String key, Set<String> set) {
        if (set == null) {
            remove(key);
        } else {
            putObject(key, set, StringSetEncoder.INSTANCE);
        }
        return this;
    }

    public synchronized Editor remove(String key) {
        lockAndCheckUpdate();
        markChanged(key);
        BaseContainer container = data.get(key);
        if (container != null) {
            String oldFileName = null;
            data.remove(key);
            byte type = container.getType();
            if (type <= DataType.DOUBLE) {
                int keySize = FastBuffer.getStringSize(key);
                int start = container.offset - (2 + keySize);
                remove(type, start, container.offset + TYPE_SIZE[type]);
            } else {
                VarContainer c = (VarContainer) container;
                remove(type, c.start, c.offset + c.valueSize);
                oldFileName = c.external ? (String) c.value : null;
            }
            if (oldFileName != null) {
                deletedFiles.add(oldFileName);
            }
            checkGC();
        }
        return this;
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

    public synchronized void force() {
        try {
            if (aBuffer != null) {
                aBuffer.force();
            }
            if (bChannel != null) {
                bChannel.force(false);
            }
        } catch (Exception e) {
            error(e);
        }
    }

    @Override
    public boolean commit() {
        return updateFile();
    }

    @Override
    public void apply() {
        applyExecutor.execute(this::updateFile);
    }

    private boolean fullWrite() {
        fastBuffer.position = 0;
        int dataSize = fastBuffer.getInt();
        checksum = fastBuffer.getLong();
        dataEnd = DATA_START + dataSize;
        if (checksum == fastBuffer.getChecksum(DATA_START, dataSize)) {
            return writeToABFile(fastBuffer);
        } else {
            clearData();
            return true;
        }
    }

    private synchronized boolean updateFile() {
        if (bFileLock == null) {
            return false;
        }
        if (fastBuffer == null || (updateCount == 0 && !needFullWrite)) {
            releaseLock();
            return false;
        }

        try {
            int dataSize = dataEnd - DATA_START;
            fastBuffer.putInt(0, dataSize);
            fastBuffer.putLong(4, checksum);

            if (needFullWrite) {
                boolean result = fullWrite();
                if (result) {
                    needFullWrite = false;
                }
                return result;
            }

            if (!alignAToBuffer()) {
                if (aBuffer != null) {
                    // If aBuffer can't align to dataBuffer (Not enough memory?)，
                    // drop the update
                    reloadFromABuffer();
                } else {
                    needFullWrite = true;
                }
                return false;
            }
            setBFileSize(aBuffer.capacity());

            // update A Aile
            //syncBufferToA(0, DATA_START);
            aBuffer.putInt(0, dataSize);
            aBuffer.putLong(4, checksum);
            for (int i = 0; i < updateCount; i += 2) {
                int start = updateStartAndSize[i];
                int size = updateStartAndSize[i + 1];
                syncBufferToA(start, size);
            }
            if (dataEnd + 8 < aBuffer.capacity()) {
                updateHash = random.nextLong() ^ System.currentTimeMillis();
                aBuffer.putLong(dataEnd, updateHash);
            }

            // update B File
            syncAToB(0, DATA_START);
            for (int i = 0; i < updateCount; i += 2) {
                int start = updateStartAndSize[i];
                int size = updateStartAndSize[i + 1];
                syncAToB(start, size);
            }

            if (!deletedFiles.isEmpty()) {
                for (String oldFileName : deletedFiles) {
                    Util.deleteFile(new File(path + name, oldFileName));
                }
            }

            if (fastBuffer.hb.length - dataEnd > TRUNCATE_THRESHOLD) {
                truncate();
            }

            return true;
        } catch (Throwable e) {
            error(e);
            needFullWrite = true;
        } finally {
            updateCount = 0;
            if (!deletedFiles.isEmpty()) {
                deletedFiles.clear();
            }
            releaseLock();
            kvHandler.sendEmptyMessage(MSG_DATA_CHANGE);
        }
        return false;
    }

    private void lockAndCheckUpdate() {
        if (bFileLock != null) {
            return;
        }
        if (bChannel == null) {
            loadFromABFile();
        }
        if (bChannel != null) {
            try {
                bFileLock = bChannel.lock();
                try {
                    checkUpdate();
                } finally {
                    // In case of user forget to release lock,
                    // set a timeout to apply data (will release lock as well).
                    kvHandler.sendEmptyMessageDelayed(MSG_APPLY, LOCK_TIMEOUT);
                }
            } catch (Throwable e) {
                error(e);
            }
        }
    }

    private void releaseLock() {
        if (bFileLock != null) {
            try {
                bFileLock.release();
            } catch (Exception e) {
                error(e);
            }
            bFileLock = null;
            kvHandler.removeMessages(MSG_APPLY);
        }
    }

    private void reloadFromABuffer() {
        if (aBuffer == null) {
            return;
        }
        reloadData();
        getUpdateHash();
        fastBuffer.position = 0;
        int dataSize = fastBuffer.getInt();
        checksum = fastBuffer.getLong();
        dataEnd = DATA_START + dataSize;
        if (checksum != fastBuffer.getChecksum(DATA_START, dataSize) || parseData() != 0) {
            clearData();
        }
    }

    private void reloadData() {
        data.clear();
        clearInvalid();
        int capacity = aBuffer.capacity();
        if (fastBuffer == null) {
            fastBuffer = new FastBuffer(capacity);
        } else if (fastBuffer.hb.length != capacity) {
            fastBuffer.hb = new byte[capacity];
        }
        aBuffer.rewind();
        aBuffer.get(fastBuffer.hb, 0, dataEnd);
    }

    private void checkUpdate() throws IOException {
        MappedByteBuffer buffer = aBuffer;
        if (buffer == null || aFile == null) {
            return;
        }
        int fileLen = (int) aFile.length();
        if (fileLen <= 0) {
            error("invalid file length");
            return;
        }
        if (aBuffer.capacity() != fileLen) {
            aChannel.truncate(fileLen);
            buffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            if (buffer == null) {
                return;
            }
            aBuffer = buffer;
            aBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        if (bChannel.size() != fileLen) {
            bChannel.truncate(fileLen);
        }
        int capacity = buffer.capacity();
        int dataSize = buffer.getInt(0);
        if (dataSize < 0 || dataSize > capacity) {
            throw new IllegalStateException("Invalid file, dataSize:" + dataSize + ", capacity:"+capacity);
        }
        long sum = buffer.getLong(4);
        int end = DATA_START + dataSize;
        long hash = updateHash;
        if (end < buffer.capacity() - 8) {
            hash = buffer.getLong(end);
        }
        if (end != dataEnd || sum != checksum || hash != updateHash) {
            dataEnd = end;
            checksum = sum;
            updateHash = hash;
            HashMap<String, BaseContainer> oldData = listeners.isEmpty() ? null : new HashMap<>(data);
            reloadData();
            if (sum == fastBuffer.getChecksum(DATA_START, dataSize) && parseData() == 0) {
                if (oldData != null) {
                    checkDiff(oldData);
                }
            } else {
                clearData();
            }
        }
    }

    private void checkDiff(HashMap<String, BaseContainer> oldData) {
        Set<String> newSet = new HashSet<>(data.keySet());
        Set<String> oldSet = new HashSet<>(oldData.keySet());
        Set<String> common = new HashSet<>(newSet);
        common.retainAll(oldSet);
        newSet.removeAll(common);
        oldSet.removeAll(common);
        changedKey.addAll(newSet);
        changedKey.addAll(oldSet);
        for (String key : common) {
            BaseContainer oldValue = oldData.get(key);
            BaseContainer newValue = data.get(key);
            if (oldValue != null && !oldValue.equalTo(newValue)) {
                changedKey.add(key);
            }
        }
        if (!changedKey.isEmpty()) {
            kvHandler.sendEmptyMessage(MSG_DATA_CHANGE);
        }
    }

    private void markChanged(String key) {
        if (!listeners.isEmpty()) {
            changedKey.add(key);
        }
    }

    /**
     * Clear all data, take effect immediately (no need to call commit/apply).
     */
    public synchronized Editor clear() {
        lockAndCheckUpdate();
        clearData();
        releaseLock();
        return this;
    }

    private void clearData() {
        resetMemory();
        try {
            alignAToBuffer();
            aBuffer.putInt(0, 0);
            aBuffer.putLong(4, 0L);
            getUpdateHash();
            if (Util.makeFileIfNotExist(bFile)) {
                setBFileSize(PAGE_SIZE);
                syncAToB(0, DATA_START);
            }
        } catch (Throwable e) {
            error(e);
            needFullWrite = true;
        }
        Util.deleteFile(new File(path + name));
        kvHandler.sendEmptyMessage(MSG_CLEAR);
    }

    private void setBFileSize(int size) throws IOException {
        if (bAccessFile == null) {
            bAccessFile = new RandomAccessFile(bFile, "rw");
        }
        if (bChannel == null) {
            bChannel = bAccessFile.getChannel();
        }
        if (bChannel.size() != size) {
            bAccessFile.setLength(size);
            bChannel.truncate(size);
        }
    }

    protected void resetData() {
        super.resetData();
        updateHash = 0;
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

    private void addUpdate(int start, int size) {
        int count = updateCount;
        int capacity = updateStartAndSize.length;
        if ((count << 1) >= capacity) {
            int[] newArray = new int[capacity << 1];
            System.arraycopy(updateStartAndSize, 0, newArray, 0, capacity);
            updateStartAndSize = newArray;
        }
        updateStartAndSize[count] = start;
        updateStartAndSize[count + 1] = size;
        updateCount = count + 2;
    }

    private void updateChange() {
        checksum ^= fastBuffer.getChecksum(updateStart, updateSize);
        if (updateSize != 0) {
            addUpdate(updateStart, updateSize);
            updateSize = 0;
        }
    }

    private void ensureSize(int allocate) {
        int capacity = fastBuffer.hb.length;
        int expected = dataEnd + allocate + 8;
        if (expected >= capacity) {
            int newCapacity = getNewCapacity(capacity, expected);
            byte[] bytes = new byte[newCapacity];
            System.arraycopy(fastBuffer.hb, 0, bytes, 0, dataEnd);
            fastBuffer.hb = bytes;
        }
    }

    private void updateBoolean(byte value, int offset) {
        checksum ^= shiftCheckSum(1L, offset);
        fastBuffer.hb[offset] = value;
        addUpdate(offset, 1);
    }

    private void updateInt32(int value, long sum, int offset) {
        checksum ^= shiftCheckSum(sum, offset);
        fastBuffer.putInt(offset, value);
        addUpdate(offset, 4);
    }

    private void updateInt64(long value, long sum, int offset) {
        checksum ^= shiftCheckSum(sum, offset);
        fastBuffer.putLong(offset, value);
        addUpdate(offset, 8);
    }

    private void updateBytes(int offset, byte[] bytes) {
        int size = bytes.length;
        checksum ^= fastBuffer.getChecksum(offset, size);
        fastBuffer.position = offset;
        fastBuffer.putBytes(bytes);
        checksum ^= fastBuffer.getChecksum(offset, size);
        addUpdate(offset, size);
    }

    private void preparePutBytes() {
        ensureSize(updateSize);
        updateStart = dataEnd;
        dataEnd += updateSize;
        fastBuffer.position = updateStart;
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
        int stringSize = FastBuffer.getStringSize(value);
        if (c == null) {
            int keySize = FastBuffer.getStringSize(key);
            checkKeySize(keySize);
            int preSize = 4 + keySize;
            updateSize = preSize + stringSize;
            preparePutBytes();
            fastBuffer.put(DataType.STRING);
            putKey(key, keySize);
            putStringValue(value, stringSize);
            data.put(key, new StringContainer(updateStart, updateStart + preSize, value, stringSize, false));
            updateChange();
        } else {
            String oldFileName = null;
            boolean needCheckGC = false;
            int preSize = c.offset - c.start;
            if (c.valueSize == stringSize) {
                checksum ^= fastBuffer.getChecksum(c.offset, c.valueSize);
                if (stringSize == value.length()) {
                    //noinspection deprecation
                    value.getBytes(0, stringSize, fastBuffer.hb, c.offset);
                } else {
                    fastBuffer.position = c.offset;
                    fastBuffer.putString(value);
                }
                updateStart = c.offset;
                updateSize = stringSize;
            } else {
                updateSize = preSize + stringSize;
                preparePutBytes();
                fastBuffer.put(DataType.STRING);
                int keyBytes = preSize - 3;
                System.arraycopy(fastBuffer.hb, c.start + 1, fastBuffer.hb, fastBuffer.position, keyBytes);
                fastBuffer.position += keyBytes;
                putStringValue(value, stringSize);

                remove(DataType.STRING, c.start, c.offset + c.valueSize);
                needCheckGC = true;
                if (c.external) {
                    oldFileName = (String) c.value;
                }
                c.external = false;
                c.start = updateStart;
                c.offset = updateStart + preSize;
                c.valueSize = stringSize;
            }
            c.value = value;
            updateChange();
            if (needCheckGC) {
                checkGC();
            }
            if (oldFileName != null) {
                deletedFiles.add(oldFileName);
            }
        }
    }

    private void addOrUpdate(String key, Object value, byte[] bytes, VarContainer c, byte type) {
        if (c == null) {
            addObject(key, value, bytes, type);
        } else {
            if (c.external || c.valueSize != bytes.length) {
                updateObject(key, value, bytes, c);
            } else {
                updateBytes(c.offset, bytes);
                c.value = value;
            }
        }
    }

    private void addObject(String key, Object value, byte[] bytes, byte type) {
        int offset = saveArray(key, bytes, type);
        if (offset != 0) {
            int size;
            Object v;
            boolean external = tempExternalName != null;
            if (external) {
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
                deletedFiles.add(oldFileName);
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
            File file = new File(path + name, fileName);
            if (Util.saveBytes(file, value)) {
                tempExternalName = fileName;
                byte[] fileNameBytes = new byte[Util.NAME_SIZE];
                //noinspection deprecation
                fileName.getBytes(0, Util.NAME_SIZE, fileNameBytes, 0);
                return wrapArray(key, fileNameBytes, (byte) (type | DataType.EXTERNAL_MASK));
            } else {
                error("save large value failed");
                return 0;
            }
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
        addUpdate(start, 1);
    }

    private void checkGC() {
        if (invalidBytes >= bytesThreshold() || invalids.size() >= BASE_GC_KEYS_THRESHOLD) {
            gc();
        }
    }

    void gc() {
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

        int minUpdateStart = gcStart;
        for (int i = 0; i < updateCount; i += 2) {
            int s = updateStartAndSize[i];
            if (s < minUpdateStart) {
                minUpdateStart = s;
            }
        }
        updateStartAndSize[0] = minUpdateStart;
        updateStartAndSize[1] = dataEnd - minUpdateStart;
        updateCount = 2;

        updateOffset(gcStart, srcToShift);
        info(GC_FINISH);
    }

    private void truncate() {
        // reserve at least one page space
        int newCapacity = getNewCapacity(PAGE_SIZE, dataEnd + PAGE_SIZE);
        if (newCapacity >= fastBuffer.hb.length) {
            return;
        }
        byte[] bytes = new byte[newCapacity];
        System.arraycopy(fastBuffer.hb, 0, bytes, 0, dataEnd);
        fastBuffer.hb = bytes;
        try {
            aChannel.truncate(newCapacity);
            aBuffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
            aBuffer.order(ByteOrder.LITTLE_ENDIAN);
            bAccessFile.setLength(newCapacity);
            bChannel.truncate(newCapacity);
        } catch (Exception e) {
            error(new Exception(MAP_FAILED, e));
            needFullWrite = true;
        }
        info(TRUNCATE_FINISH);
    }

    private synchronized void refresh() {
        lockAndCheckUpdate();
        releaseLock();
    }

    private final Handler kvHandler = new Handler(Looper.getMainLooper()) {
        @Override
        public void handleMessage(Message msg) {
            switch (msg.what) {
                case MSG_REFRESH:
                    refreshExecutor.execute(MPFastKV.this::refresh);
                    break;
                case MSG_APPLY:
                    apply();
                    break;
                case MSG_DATA_CHANGE:
                    notifyChangedKeys();
                    break;
                case MSG_CLEAR:
                    synchronized (MPFastKV.this) {
                        notifyListeners(listeners, null);
                    }
                    break;
                default:
                    break;
            }
        }
    };

    private synchronized void notifyChangedKeys() {
        for (String key : changedKey) {
            notifyListeners(listeners, key);
        }
        changedKey.clear();
    }

    private class KVFileObserver extends FileObserver {
        public KVFileObserver(String path) {
            super(path, FileObserver.MODIFY);
        }

        @Override
        public void onEvent(int event, String path) {
            // Delay a few time to filter frequency callbacks.
            if (!kvHandler.hasMessages(MSG_REFRESH)) {
                kvHandler.sendEmptyMessageDelayed(MSG_REFRESH, 30L);
            }
        }
    }

    public static class Builder {
        private static final Map<String, MPFastKV> INSTANCE_MAP = new ConcurrentHashMap<>();
        private final String path;
        private final String name;
        private Encoder[] encoders;
        private boolean needWatchFileChange = true;

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
         * If there are multi processes access one file, we need to watch the file's changes generally.
         * But if only one process will update the file,
         * that process is unnecessary to watch changes (other processes won't change the file).
         * In that case, call this method will benefit to efficiency.
         *
         * @return the builder
         */
        public Builder disableWatchFileChange() {
            needWatchFileChange = false;
            return this;
        }

        public MPFastKV build() {
            String key = path + name;
            MPFastKV kv = INSTANCE_MAP.get(key);
            if (kv == null) {
                synchronized (Builder.class) {
                    kv = INSTANCE_MAP.get(key);
                    if (kv == null) {
                        kv = new MPFastKV(path, name, encoders, needWatchFileChange);
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
        return "MPFastKV: path:" + path + " name:" + name;
    }
}
