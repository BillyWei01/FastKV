package io.fastkv;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Handler;
import android.os.Looper;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import io.fastkv.Container.*;
import io.fastkv.interfaces.*;

/**
 * FastKV has three writing mode: <br>
 * non-blocking: write partial data with mmap). <br>
 * async-block: write all data to disk with blocking I/O asynchronously, likes 'apply' of  SharePreferences.<br>
 * sync-block: write all data too, but it likes 'commit' of  SharePreferences.<br>
 * <br>
 * Note: <br>
 * 1. Do not change file name once create. <br>
 * 2. Do not change cipher once create.
 *    But it's okay to apply cipher from the state of no cipher.<br>
 * 3. Do not change value type for one key.<br>
 */
@SuppressWarnings("rawtypes")
public final class FastKV implements SharedPreferences, SharedPreferences.Editor {
    // Constants from AbsFastKV
    private static final String BOTH_FILES_ERROR = "both files error";
    private static final String PARSE_DATA_FAILED = "parse dara failed";
    private static final String OPEN_FILE_FAILED = "open file failed";
    private static final String MAP_FAILED = "map failed";
    private static final String MISS_CIPHER = "miss cipher";
    private static final String ENCRYPT_FAILED = "Encrypt failed";

    static final String TRUNCATE_FINISH = "truncate finish";
    static final String GC_FINISH = "gc finish";

    private static final String A_SUFFIX = ".kva";
    private static final String B_SUFFIX = ".kvb";
    private static final String C_SUFFIX = ".kvc";
    private static final String TEMP_SUFFIX = ".tmp";

    private static final int DATA_SIZE_LIMIT = 1 << 28; // 256M
    private static final int CIPHER_MASK = 1 << 30;

    private static final byte[] EMPTY_ARRAY = new byte[0];
    private static final int[] TYPE_SIZE = {0, 1, 4, 4, 8, 8};
    private static final int DATA_START = 12;

    private static final int PAGE_SIZE = Utils.getPageSize();
    private static final int TRUNCATE_THRESHOLD = Math.max(PAGE_SIZE, 1 << 15);

    private static final int BASE_GC_BYTES_THRESHOLD = 8192;
    private static final int BASE_GC_KEYS_THRESHOLD = 80;

    // Fields from AbsFastKV
    private final String path;
    private final String name;
    private final Map<String, FastEncoder> encoderMap;
    private final FastLogger logger = FastKVConfig.sLogger;
    private final FastCipher cipher;

    private int dataEnd;
    private long checksum;
    private final HashMap<String, BaseContainer> data = new HashMap<>();
    private volatile boolean startLoading = false;

    private FastBuffer fastBuffer;
    private int updateStart;
    private int updateSize;

    private final List<String> deletedFiles = new ArrayList<>();

    // If the kv had not encrypted before, and need to encrypt this time,
    // It has to rewrite the data.
    private boolean needRewrite = false;

    private boolean closed = false;

    private final WeakCache bigValueCache = new WeakCache();

    private final Executor applyExecutor = new LimitExecutor();

    private int invalidBytes;
    private final ArrayList<Segment> invalids = new ArrayList<>();

    private final ArrayList<SharedPreferences.OnSharedPreferenceChangeListener> listeners = new ArrayList<>();
    private final Handler mainHandler = new Handler(Looper.getMainLooper());

    // Original FastKV fields
    private FileChannel aChannel;
    private FileChannel bChannel;
    private MappedByteBuffer aBuffer;
    private MappedByteBuffer bBuffer;

    private int removeStart;

    // The default writing mode is non-blocking (write partial data with mmap).
    // If mmap API throw IOException, degrade to blocking mode (write all data to disk with blocking I/O).
    // User could assign to using blocking mode by FastKV.Builder
    static final int NON_BLOCKING = 0;
    static final int ASYNC_BLOCKING = 1;
    static final int SYNC_BLOCKING = 2;
    private int writingMode;

    // Only take effect when mode is not NON_BLOCKING
    boolean autoCommit = true;

    FastKV(final String path,
           final String name,
           FastEncoder[] encoders,
           FastCipher cipher,
           int writingMode) {
        this.path = path;
        this.name = name;
        this.cipher = cipher;
        this.writingMode = writingMode;
        
        Map<String, FastEncoder> map = new HashMap<>();
        if (encoders != null) {
            for (FastEncoder e : encoders) {
                String tag = e.tag();
                if (map.containsKey(tag)) {
                    error("duplicate encoder tag:" + tag);
                } else {
                    map.put(tag, e);
                }
            }
        }
        StringSetEncoder encoder = StringSetEncoder.INSTANCE;
        map.put(encoder.tag(), encoder);
        this.encoderMap = map;

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
        // Once obtained the object lock, notify the waiter to continue the constructor
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
        if (dataEnd == 0) {
            dataEnd = DATA_START;
        }
        if (needRewrite) {
            rewrite();
            info("rewrite data");
        }
        if (logger != null) {
            long t = (System.nanoTime() - start) / 1000000;
            info("loading finish, data len:" + dataEnd + ", get keys:" + data.size() + ", use time:" + t + " ms");
        }
    }

    // Utility methods from AbsFastKV
    private int packSize(int size) {
        return cipher == null ? size : size | CIPHER_MASK;
    }

    private static int unpackSize(int size) {
        return size & (~CIPHER_MASK);
    }

    private static boolean isCipher(int size) {
        return (size & CIPHER_MASK) != 0;
    }

    private int getNewCapacity(int capacity, int expected) {
        if (expected >= DATA_SIZE_LIMIT) {
            throw new IllegalStateException("data size out of limit");
        }
        if (expected <= PAGE_SIZE) {
            return PAGE_SIZE;
        } else {
            while (capacity < expected) {
                capacity <<= 1;
            }
            return capacity;
        }
    }

    /**
     * Rewrite data, from non-encrypt to encrypted.
     */
    private void rewrite() {
        FastEncoder[] encoders = new FastEncoder[encoderMap.size()];
        encoders = encoderMap.values().toArray(encoders);
        String tempName = "temp_" + name;

        // Here we use FastKV with blocking mode and close 'autoCommit',
        // to make data only keep on memory.
        FastKV tempKV = new FastKV(path, tempName, encoders, cipher, SYNC_BLOCKING);
        tempKV.autoCommit = false;

        List<String> oldExternalFiles = new ArrayList<>();
        for (Map.Entry<String, BaseContainer> entry : data.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof StringContainer) {
                StringContainer c = (StringContainer) value;
                if (c.external) {
                    oldExternalFiles.add((String) c.value);
                    String bigStr = getStringFromFile(c, null);
                    if (bigStr != null) {
                        tempKV.putString(key, bigStr);
                    }
                } else {
                    tempKV.putString(key, (String) c.value);
                }
            } else if (value instanceof BooleanContainer) {
                tempKV.putBoolean(key, ((BooleanContainer) value).value);
            } else if (value instanceof IntContainer) {
                tempKV.putInt(key, ((IntContainer) value).value);
            } else if (value instanceof LongContainer) {
                tempKV.putLong(key, ((LongContainer) value).value);
            } else if (value instanceof FloatContainer) {
                tempKV.putFloat(key, ((FloatContainer) value).value);
            } else if (value instanceof DoubleContainer) {
                tempKV.putDouble(key, ((DoubleContainer) value).value);
            } else if (value instanceof ArrayContainer) {
                ArrayContainer c = (ArrayContainer) value;
                if (c.external) {
                    oldExternalFiles.add((String) c.value);
                    byte[] bigArray = getArrayFromFile(c, null);
                    if (bigArray != null) {
                        tempKV.putArray(key, bigArray);
                    }
                } else {
                    tempKV.putArray(key, (byte[]) c.value);
                }
            } else if (value instanceof ObjectContainer) {
                ObjectContainer c = (ObjectContainer) value;
                if (c.external) {
                    oldExternalFiles.add((String) c.value);
                    Object obj = getObjectFromFile(c, null);
                    if (obj != null && c.encoder != null) {
                        //noinspection unchecked
                        tempKV.putObject(key, obj, c.encoder);
                    }
                } else if (c.encoder != null) {
                    //noinspection unchecked
                    tempKV.putObject(key, c.value, c.encoder);
                }
            }
        }

        // 'loadData()' of FastKV is a method execute in async thread, and it's a synchronized method.
        //  To ensure tempKV loading finish,
        //  calling another synchronized method to block current process (if loading not finish).
        //noinspection ResultOfMethodCallIgnored
        tempKV.contains("");

        // Copy memory data
        fastBuffer = tempKV.fastBuffer;
        checksum = tempKV.checksum;
        dataEnd = tempKV.dataEnd;
        clearInvalid();
        data.clear();
        data.putAll(tempKV.data);

        copyToMainFile(tempKV);

        // Move external files
        File tempDir = new File(path, tempName);
        String currentDir = path + name;
        Utils.moveDirFiles(tempDir, currentDir);
        Utils.deleteFile(tempDir);
        for (String name : oldExternalFiles) {
            Utils.deleteFile(new File(currentDir, name));
        }

        needRewrite = false;
    }

    private void copyToMainFile(FastKV tempKV) {
        FastBuffer buffer = tempKV.fastBuffer;
        if (writingMode == NON_BLOCKING) {
            int capacity = buffer.hb.length;
            if (aBuffer != null && aBuffer.capacity() == capacity
                    && bBuffer != null && bBuffer.capacity() == capacity) {
                aBuffer.position(0);
                aBuffer.put(buffer.hb, 0, dataEnd);
                bBuffer.position(0);
                bBuffer.put(buffer.hb, 0, dataEnd);
            } else {
                if (!writeToABFile(buffer)) {
                    writingMode = ASYNC_BLOCKING;
                }
            }
        }
        if (writingMode != NON_BLOCKING) {
            writeToCFile();
        }
    }


    @SuppressWarnings("resource")
    private void loadFromABFile() {
        File aFile = new File(path, name + A_SUFFIX);
        File bFile = new File(path, name + B_SUFFIX);
        try {
            if (!Utils.makeFileIfNotExist(aFile) || !Utils.makeFileIfNotExist(bFile)) {
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
                int aSize = aBuffer.getInt();
                int aDataSize = unpackSize(aSize);
                boolean aHadEncrypted = isCipher(aSize);
                long aCheckSum = aBuffer.getLong();

                int bSize = bBuffer.getInt();
                int bDataSize = unpackSize(bSize);
                boolean bHadEncrypted = isCipher(bSize);
                long bCheckSum = bBuffer.getLong();

                boolean isAValid = false;
                if (aDataSize >= 0 && (aDataSize <= aFileLen - DATA_START)) {
                    dataEnd = DATA_START + aDataSize;
                    aBuffer.rewind();
                    aBuffer.get(fastBuffer.hb, 0, dataEnd);
                    if (aCheckSum == fastBuffer.getChecksum(DATA_START, aDataSize) && parseData(aHadEncrypted)) {
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
                        if (bCheckSum == fastBuffer.getChecksum(DATA_START, bDataSize) && parseData(bHadEncrypted)) {
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
        RandomAccessFile aAccessFile = null;
        RandomAccessFile bAccessFile = null;
        try {
            int fileLen = buffer.hb.length;
            File aFile = new File(path, name + A_SUFFIX);
            File bFile = new File(path, name + B_SUFFIX);
            if (!Utils.makeFileIfNotExist(aFile) || !Utils.makeFileIfNotExist(bFile)) {
                throw new Exception(OPEN_FILE_FAILED);
            }
            aAccessFile = new RandomAccessFile(aFile, "rw");
            aAccessFile.setLength(fileLen);
            aChannel = aAccessFile.getChannel();
            aBuffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            aBuffer.order(ByteOrder.LITTLE_ENDIAN);
            aBuffer.put(buffer.hb, 0, dataEnd);

            bAccessFile = new RandomAccessFile(bFile, "rw");
            bAccessFile.setLength(fileLen);
            bChannel = bAccessFile.getChannel();
            bBuffer = bChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            bBuffer.order(ByteOrder.LITTLE_ENDIAN);
            bBuffer.put(buffer.hb, 0, dataEnd);
            return true;
        } catch (Exception e) {
            Utils.closeQuietly(aAccessFile);
            Utils.closeQuietly(bAccessFile);
            aChannel = null;
            bChannel = null;
            aBuffer = null;
            bBuffer = null;
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

    public synchronized Editor remove(String key) {
        if (closed) return this;
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
                    deleteExternalFile(oldFileName);
                } else {
                    deletedFiles.add(oldFileName);
                }
            }
            checkGC();
            checkIfCommit();
        }
        return this;
    }

    public synchronized Editor clear() {
        if (closed) return this;
        clearData();
        if (writingMode != NON_BLOCKING) {
            deleteCFiles();
        }
        notifyListeners(null);
        return this;
    }

    /**
     * Batch put objects.
     * Only support type in [boolean, int, long, float, double, String, byte[], Set of String] and object with encoder.
     *
     * @param values   map of key to value
     * @param encoders map of value Class to Encoder
     */
    public synchronized void putAll(Map<String, Object> values, Map<Class, FastEncoder> encoders) {
        if (closed) return;
        if (writingMode != NON_BLOCKING) {
            autoCommit = false;
        }
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
                } else if (value instanceof byte[]) {
                    putArray(key, (byte[]) value);
                } else {
                    encodeObject(key, value, encoders);
                }
            }
        }
        if (writingMode != NON_BLOCKING) {
            commit();
        }
    }

    public void putAll(Map<String, Object> values) {
        putAll(values, null);
    }

    private void encodeObject(String key, Object value, Map<Class, FastEncoder> encoders) {
        if (value instanceof Set) {
            Set set = (Set) value;
            if (set.isEmpty() || set.iterator().next() instanceof String) {
                //noinspection unchecked
                putStringSet(key, set);
                return;
            }
        }
        if (encoders != null) {
            FastEncoder encoder = encoders.get(value.getClass());
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

    /**
     * Forces any changes to be written to the storage device containing the mapped file.
     * No need to call this unless what's had written is very import.
     * The system crash or power off before data syncing to disk might make recently update lost.
     */
    public synchronized void force() {
        if (closed) return;
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
        if (closed) return false;
        autoCommit = true;
        return commitToCFile();
    }

    @Override
    public synchronized void apply() {
        if (closed) return;
        autoCommit = true;
        commitToCFile();
    }

    private void handleChange(String key) {
        checkIfCommit();
        notifyListeners(key);
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
            if (Utils.makeFileIfNotExist(tmpFile)) {
                try (RandomAccessFile accessFile = new RandomAccessFile(tmpFile, "rw")) {
                    accessFile.setLength(dataEnd);
                    accessFile.write(fastBuffer.hb, 0, dataEnd);
                    accessFile.getFD().sync();
                }
                File cFile = new File(path, name + C_SUFFIX);
                if (Utils.renameFile(tmpFile, cFile)) {
                    clearDeletedFiles();
                    return true;
                } else {
                    warning(new Exception("rename failed"));
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
                deleteExternalFile(oldFileName);
            }
            deletedFiles.clear();
        }
    }

    private void toBlockingMode() {
        writingMode = ASYNC_BLOCKING;
        Utils.closeQuietly(aChannel);
        Utils.closeQuietly(bChannel);
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
            } catch (Exception e) {
                toBlockingMode();
            }
        }
        resetMemory();
        Utils.deleteFile(new File(path + name));
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
        buffer.putInt(0, packSize(0));
        buffer.putLong(4, 0L);
    }

    private void updateChange() {
        checksum ^= fastBuffer.getChecksum(updateStart, updateSize);
        int packedSize = packSize(dataEnd - DATA_START);
        if (writingMode == NON_BLOCKING) {
            // When size of changed data is more than 8 bytes,
            // checksum might fail to check the integrity in small probability.
            // So we make the dataLen to be negative,
            // if crash happen when writing data to mmap memory,
            // we can know that the writing had not accomplished.
            aBuffer.putInt(0, -1);
            syncToABBuffer(aBuffer);
            aBuffer.putInt(0, packedSize);

            // bBuffer doesn't need to mark dataLen's part before writing bytes,
            // cause aBuffer has already written completely.
            // We just need to have one file to be integrated at least at any time.
            bBuffer.putInt(0, packedSize);
            syncToABBuffer(bBuffer);
        } else {
            fastBuffer.putInt(0, packedSize);
            fastBuffer.putLong(4, checksum);
        }
        removeStart = 0;
        updateSize = 0;
    }

    private void syncToABBuffer(MappedByteBuffer buffer) {
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
                        fastBuffer.putInt(0, packSize(dataEnd - DATA_START));
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
            aBuffer.putInt(0, packSize(dataEnd - DATA_START));
            bBuffer.putLong(4, checksum);
            bBuffer.position(offset);
            bBuffer.put(bytes);
        } else {
            fastBuffer.putLong(4, checksum);
        }
    }

    private void removeOldFile(String oldFileName) {
        if (writingMode == NON_BLOCKING) {
            deleteExternalFile(oldFileName);
        } else {
            deletedFiles.add(oldFileName);
        }
    }

    private void remove(byte type, int start, int end) {
        invalidBytes += (end - start);
        invalids.add(new Segment(start, end));
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

    private void updateBuffer(int gcStart, int allocate, int gcUpdateSize) {
        int newDataSize = dataEnd - DATA_START;
        int packedSize = packSize(newDataSize);
        if (writingMode == NON_BLOCKING) {
            aBuffer.putInt(0, -1);
            aBuffer.putLong(4, checksum);
            aBuffer.position(gcStart);
            aBuffer.put(fastBuffer.hb, gcStart, gcUpdateSize);
            aBuffer.putInt(0, packedSize);
            bBuffer.putInt(0, packedSize);
            bBuffer.putLong(4, checksum);
            bBuffer.position(gcStart);
            bBuffer.put(fastBuffer.hb, gcStart, gcUpdateSize);
        } else {
            fastBuffer.putInt(0, packedSize);
            fastBuffer.putLong(4, checksum);
        }

        int expectedEnd = dataEnd + allocate;
        if (fastBuffer.hb.length - expectedEnd > TRUNCATE_THRESHOLD) {
            truncate(expectedEnd);
        }
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
            } catch (Exception e) {
                error(new Exception(MAP_FAILED, e));
                toBlockingMode();
            }
        }
        info(TRUNCATE_FINISH);
    }

    private boolean loadWithBlockingIO(@NonNull File srcFile) throws IOException {
        long fileLen = srcFile.length();
        if (fileLen == 0 || fileLen >= DATA_SIZE_LIMIT) {
            return false;
        }
        int fileSize = (int) fileLen;
        int capacity = getNewCapacity(PAGE_SIZE, fileSize);
        FastBuffer buffer;
        if (fastBuffer != null && fastBuffer.hb.length == capacity) {
            buffer = fastBuffer;
            buffer.position = 0;
        } else {
            buffer = new FastBuffer(new byte[capacity]);
            fastBuffer = buffer;
        }
        Utils.readBytes(srcFile, buffer.hb, fileSize);
        int size = buffer.getInt();
        if (size < 0) {
            return false;
        }
        int dataSize = unpackSize(size);
        boolean hadEncrypted = isCipher(size);
        long sum = buffer.getLong();
        dataEnd = DATA_START + dataSize;
        if (dataSize >= 0 && (dataSize <= fileSize - DATA_START)
                && sum == buffer.getChecksum(DATA_START, dataSize)
                && parseData(hadEncrypted)) {
            checksum = sum;
            return true;
        }
        return false;
    }

    private void deleteExternalFile(String fileName) {
        // 直接删除外部文件，用于向前兼容清理
        FastKVConfig.getExecutor().execute(() -> Utils.deleteFile(new File(path + name, fileName)));
    }

    private void deleteCFiles() {
        try {
            Utils.deleteFile(new File(path, name + C_SUFFIX));
            Utils.deleteFile(new File(path, name + TEMP_SUFFIX));
        } catch (Exception e) {
            error(e);
        }
    }

    private boolean parseData(boolean hadEncrypted) {
        if (hadEncrypted && cipher == null) {
            error(MISS_CIPHER);
            return false;
        }
        FastCipher dataCipher = hadEncrypted ? cipher : null;
        FastBuffer buffer = fastBuffer;
        buffer.position = DATA_START;
        try {
            while (buffer.position < dataEnd) {
                int start = buffer.position;
                byte info = buffer.get();
                byte type = (byte) (info & DataType.TYPE_MASK);
                if (type < DataType.BOOLEAN || type > DataType.OBJECT) {
                    throw new Exception(PARSE_DATA_FAILED);
                }
                int keySize = buffer.get() & 0xFF;
                if (keySize == 0) {
                    throw new IllegalStateException("invalid key size");
                }
                if (info < 0) {
                    buffer.position += keySize;
                    int valueSize = (type <= DataType.DOUBLE) ? TYPE_SIZE[type] : buffer.getShort() & 0xFFFF;
                    buffer.position += valueSize;
                    countInvalid(start, buffer.position);
                    continue;
                }
                String key = buffer.getString(dataCipher, keySize);
                int pos = buffer.position;
                if (type <= DataType.DOUBLE) {
                    switch (type) {
                        case DataType.BOOLEAN:
                            data.put(key, new BooleanContainer(pos, buffer.get() == 1));
                            break;
                        case DataType.INT:
                            data.put(key, new IntContainer(pos, buffer.getInt(dataCipher)));
                            break;
                        case DataType.LONG:
                            data.put(key, new LongContainer(pos, buffer.getLong(dataCipher)));
                            break;
                        case DataType.FLOAT:
                            data.put(key, new FloatContainer(pos, buffer.getFloat(dataCipher)));
                            break;
                        default:
                            data.put(key, new DoubleContainer(pos, buffer.getDouble(dataCipher)));
                            break;
                    }
                } else {
                    int size = buffer.getShort() & 0xFFFF;
                    boolean external = (info & DataType.EXTERNAL_MASK) != 0;
                    if (external && size != Utils.NAME_SIZE) {
                        throw new IllegalStateException("name size not match");
                    }
                    switch (type) {
                        case DataType.STRING:
                            String str = external ? buffer.getString(size) : buffer.getString(dataCipher, size);
                            data.put(key, new StringContainer(start, pos + 2, str, size, external));
                            break;
                        case DataType.ARRAY:
                            Object value = external ? buffer.getString(size) : buffer.getBytes(dataCipher, size);
                            data.put(key, new ArrayContainer(start, pos + 2, value, size, external));
                            break;
                        default:
                            if (external) {
                                String fileName = buffer.getString(size);
                                data.put(key, new ObjectContainer(start, pos + 2, fileName, size, true));
                            } else {
                                parseObject(size, key, start, pos, dataCipher);
                                buffer.position = pos + 2 + size;
                            }
                            break;
                    }
                }
            }
        } catch (Exception e) {
            error(e);
            return false;
        }
        if (buffer.position != dataEnd) {
            error(new Exception(PARSE_DATA_FAILED));
            return false;
        }
        needRewrite = !hadEncrypted && cipher != null && dataEnd != DATA_START;
        return true;
    }

    private void parseObject(int size, String key, int start, int pos,
                             FastCipher dataCipher) throws Exception {
        FastBuffer buffer;
        int dataLen;
        if (dataCipher == null) {
            buffer = fastBuffer;
            dataLen = size;
        } else {
            byte[] bytes = new byte[size];
            System.arraycopy(fastBuffer.hb, fastBuffer.position, bytes, 0, size);
            byte[] dstBytes = dataCipher.decrypt(bytes);
            buffer = new FastBuffer(dstBytes);
            dataLen = dstBytes.length;
        }
        int tagSize = buffer.get() & 0xFF;
        String tag = buffer.getString(tagSize);
        FastEncoder encoder = encoderMap.get(tag);
        int objectSize = dataLen - (tagSize + 1);
        if (objectSize < 0) {
            throw new Exception(PARSE_DATA_FAILED);
        }
        if (encoder != null) {
            try {
                Object obj = encoder.decode(buffer.hb, buffer.position, objectSize);
                if (obj != null) {
                    ObjectContainer container = new ObjectContainer(start, pos + 2, obj, size, false);
                    container.encoder = encoder;
                    data.put(key, container);
                }
            } catch (Exception e) {
                error(e);
            }
        } else {
            error("object with tag: " + tag + " without encoder");
        }
    }

    private void checkKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is empty");
        }
    }

    static class Segment implements Comparable<Segment> {
        int start;
        int end;

        Segment(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public int compareTo(Segment o) {
            return start - o.start;
        }
    }

    /**
     * Merge invalids to speed up compacting bytes.
     */
    static void mergeInvalids(ArrayList<Segment> invalids) {
        Collections.sort(invalids);
        int index = 0;
        Segment p = invalids.get(0);
        int n = invalids.size();
        for (int i = 1; i < n; i++) {
            Segment q = invalids.get(i);
            if (q.start == p.end) {
                p.end = q.end;
            } else {
                index++;
                if (index != i) {
                    invalids.set(index, q);
                }
                p = q;
            }
        }
        index++;
        if (n > index) {
            invalids.subList(index, n).clear();
        }
    }

    private void gc(int allocate) {
        mergeInvalids(invalids);

        final Segment head = invalids.get(0);
        final int gcStart = head.start;
        final int newDataEnd = dataEnd - invalidBytes;
        final int newDataSize = newDataEnd - DATA_START;
        final int gcUpdateSize = newDataEnd - gcStart;
        final int gcSize = dataEnd - gcStart;
        final boolean fullChecksum = newDataSize < gcSize + gcUpdateSize;
        if (!fullChecksum) {
            checksum ^= fastBuffer.getChecksum(gcStart, gcSize);
        }
        // compact and record shift
        int n = invalids.size();
        final int remain = dataEnd - invalids.get(n - 1).end;
        int shiftCount = (remain > 0) ? n : n - 1;
        int[] src = new int[shiftCount];
        int[] shift = new int[shiftCount];
        int desPos = head.start;
        int srcPos = head.end;
        for (int i = 1; i < n; i++) {
            Segment q = invalids.get(i);
            int size = q.start - srcPos;
            System.arraycopy(fastBuffer.hb, srcPos, fastBuffer.hb, desPos, size);
            int index = i - 1;
            src[index] = srcPos;
            shift[index] = srcPos - desPos;
            desPos += size;
            srcPos = q.end;
        }
        if (remain > 0) {
            System.arraycopy(fastBuffer.hb, srcPos, fastBuffer.hb, desPos, remain);
            int index = n - 1;
            src[index] = srcPos;
            shift[index] = srcPos - desPos;
        }
        clearInvalid();

        if (fullChecksum) {
            checksum = fastBuffer.getChecksum(DATA_START, newDataEnd - DATA_START);
        } else {
            checksum ^= fastBuffer.getChecksum(gcStart, newDataEnd - gcStart);
        }
        dataEnd = newDataEnd;

        updateBuffer(gcStart, allocate, gcUpdateSize);

        updateOffset(gcStart, src, shift);

        info(GC_FINISH);
    }

    private void updateOffset(int gcStart, int[] srcArray, int[] shiftArray) {
        Collection<BaseContainer> values = data.values();
        for (BaseContainer c : values) {
            if (c.offset > gcStart) {
                int index = Utils.binarySearch(srcArray, c.offset);
                int shift = shiftArray[index];
                c.offset -= shift;
                if (c.getType() >= DataType.STRING) {
                    ((VarContainer) c).start -= shift;
                }
            }
        }
    }

    private void tryBlockingIO(File aFile, File bFile) {
        try {
            if (loadWithBlockingIO(aFile)) {
                return;
            }
        } catch (IOException e) {
            warning(e);
        }
        resetMemory();
        try {
            if (loadWithBlockingIO(bFile)) {
                return;
            }
        } catch (IOException e) {
            warning(e);
        }
        resetMemory();
    }

    private void resetMemory() {
        dataEnd = DATA_START;
        checksum = 0L;
        data.clear();
        bigValueCache.clear();
        clearInvalid();
        resetBuffer();
    }

    private void resetBuffer() {
        if (fastBuffer == null || fastBuffer.hb.length != PAGE_SIZE) {
            fastBuffer = new FastBuffer(PAGE_SIZE);
        } else {
            fastBuffer.putLong(4, 0L);
        }
        fastBuffer.putInt(0, packSize(0));
    }

    private void countInvalid(int start, int end) {
        invalidBytes += (end - start);
        invalids.add(new Segment(start, end));
    }

    private void clearInvalid() {
        invalidBytes = 0;
        invalids.clear();
    }

    private void error(String message) {
        if (logger != null) {
            logger.e(name, new Exception(message));
        }
    }

    private void error(Exception e) {
        if (logger != null) {
            logger.e(name, e);
        }
    }

    private void warning(Exception e) {
        if (logger != null) {
            logger.w(name, e);
        }
    }

    private void info(String message) {
        if (logger != null) {
            logger.i(name, message);
        }
    }

    private int bytesThreshold() {
        if (dataEnd <= (1 << 14)) {
            return BASE_GC_BYTES_THRESHOLD;
        } else {
            return BASE_GC_BYTES_THRESHOLD << 1;
        }
    }

    private long shiftCheckSum(long checkSum, int offset) {
        int shift = (offset & 7) << 3;
        return (checkSum << shift) | (checkSum >>> (64 - shift));
    }

    // SharedPreferences interface methods
    public synchronized boolean contains(String key) {
        return data.containsKey(key);
    }

    public synchronized boolean getBoolean(String key) {
        return getBoolean(key, false);
    }

    public synchronized boolean getBoolean(String key, boolean defValue) {
        BaseContainer c = data.get(key);
        return c == null || c.getType() != DataType.BOOLEAN ? defValue : ((BooleanContainer) c).value;
    }

    public int getInt(String key) {
        return getInt(key, 0);
    }

    public synchronized int getInt(String key, int defValue) {
        BaseContainer c = data.get(key);
        return c == null || c.getType() != DataType.INT ? defValue : ((IntContainer) c).value;
    }

    public float getFloat(String key) {
        return getFloat(key, 0f);
    }

    public synchronized float getFloat(String key, float defValue) {
        BaseContainer c = data.get(key);
        return c == null || c.getType() != DataType.FLOAT ? defValue : ((FloatContainer) c).value;
    }

    public long getLong(String key) {
        return getLong(key, 0L);
    }

    public synchronized long getLong(String key, long defValue) {
        BaseContainer c = data.get(key);
        return c == null || c.getType() != DataType.LONG ? defValue : ((LongContainer) c).value;
    }

    public double getDouble(String key) {
        return getDouble(key, 0D);
    }

    public synchronized double getDouble(String key, double defValue) {
        BaseContainer c = data.get(key);
        return c == null || c.getType() != DataType.DOUBLE ? defValue : ((DoubleContainer) c).value;
    }

    public String getString(String key) {
        return getString(key, "");
    }

    public synchronized String getString(String key, String defValue) {
        BaseContainer container = data.get(key);
        if (container == null || container.getType() != DataType.STRING) {
            return defValue;
        }
        StringContainer c = (StringContainer) container;
        if (c.external) {
            Object value = bigValueCache.get(key);
            if (value instanceof String) {
                return (String) value;
            }
            String str = getStringFromFile(c, cipher);
            if (str == null || str.isEmpty()) {
                remove(key);
                return defValue;
            } else {
                bigValueCache.put(key, str);
                return str;
            }
        } else {
            return (String) c.value;
        }
    }

    private String getStringFromFile(StringContainer c, FastCipher fastCipher) {
        String fileName = (String) c.value;
        try {
            // 向前兼容：仍然支持读取已存在的外部文件
            byte[] bytes = Utils.getBytes(new File(path + name, fileName));
            if (bytes != null) {
                bytes = fastCipher != null ? fastCipher.decrypt(bytes) : bytes;
                return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
            }
        } catch (Exception e) {
            error(e);
        }
        return null;
    }

    public byte[] getArray(String key) {
        return getArray(key, EMPTY_ARRAY);
    }

    public synchronized byte[] getArray(String key, byte[] defValue) {
        BaseContainer container = data.get(key);
        if (container == null || container.getType() != DataType.ARRAY) {
            return defValue;
        }
        ArrayContainer c = (ArrayContainer) container;
        if (c.external) {
            Object value = bigValueCache.get(key);
            if (value instanceof byte[]) {
                return (byte[]) value;
            }
            byte[] bytes = getArrayFromFile(c, cipher);
            if (bytes == null || bytes.length == 0) {
                remove(key);
                return defValue;
            } else {
                bigValueCache.put(key, bytes);
                return bytes;
            }
        } else {
            return (byte[]) c.value;
        }
    }

    private byte[] getArrayFromFile(ArrayContainer c, FastCipher fastCipher) {
        String fileName = (String) c.value;
        try {
            // 向前兼容：仍然支持读取已存在的外部文件
            byte[] bytes = Utils.getBytes(new File(path + name, fileName));
            if (bytes != null) {
                return fastCipher != null ? fastCipher.decrypt(bytes) : bytes;
            }
        } catch (Exception e) {
            error(e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> T getObject(String key) {
        BaseContainer container = data.get(key);
        if (container == null || container.getType() != DataType.OBJECT) {
            return null;
        }
        ObjectContainer c = (ObjectContainer) container;
        if (c.external) {
            Object value = bigValueCache.get(key);
            if (value != null) {
                return (T) value;
            }
            Object obj = getObjectFromFile(c, cipher);
            if (obj == null) {
                remove(key);
                return null;
            } else {
                bigValueCache.put(key, obj);
                return (T) obj;
            }
        } else {
            return (T) c.value;
        }
    }

    private Object getObjectFromFile(ObjectContainer c, FastCipher fastCipher) {
        String fileName = (String) c.value;
        try {
            // 向前兼容：仍然支持读取已存在的外部文件
            byte[] bytes = Utils.getBytes(new File(path + name, fileName));
            if (bytes != null) {
                bytes = fastCipher != null ? fastCipher.decrypt(bytes) : bytes;
                int tagSize = bytes[0] & 0xFF;
                String tag = fastBuffer.decodeStr(bytes, 1, tagSize);
                FastEncoder encoder = encoderMap.get(tag);
                if (encoder != null) {
                    c.encoder = encoder;
                    int offset = 1 + tagSize;
                    return encoder.decode(bytes, offset, bytes.length - offset);
                } else {
                    warning(new Exception("No encoder for tag:" + tag));
                }
            } else {
                warning(new Exception("Read object data failed"));
            }
        } catch (Exception e) {
            error(e);
        }
        return null;
    }

    public synchronized Set<String> getStringSet(String key) {
        return getObject(key);
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

    @Override
    public synchronized Map<String, Object> getAll() {
        int size = data.size();
        if (size == 0) {
            return new HashMap<>();
        }
        Map<String, Object> result = new HashMap<>(size * 4 / 3 + 1);
        for (Map.Entry<String, BaseContainer> entry : data.entrySet()) {
            String key = entry.getKey();
            BaseContainer c = entry.getValue();
            Object value = null;
            switch (c.getType()) {
                case DataType.BOOLEAN:
                    value = ((BooleanContainer) c).value;
                    break;
                case DataType.INT:
                    value = ((IntContainer) c).value;
                    break;
                case DataType.FLOAT:
                    value = ((FloatContainer) c).value;
                    break;
                case DataType.LONG:
                    value = ((LongContainer) c).value;
                    break;
                case DataType.DOUBLE:
                    value = ((DoubleContainer) c).value;
                    break;
                case DataType.STRING:
                    StringContainer sc = (StringContainer) c;
                    value = sc.external ? getStringFromFile(sc, cipher) : sc.value;
                    break;
                case DataType.ARRAY:
                    ArrayContainer ac = (ArrayContainer) c;
                    value = ac.external ? getArrayFromFile(ac, cipher) : ac.value;
                    break;
                case DataType.OBJECT:
                    ObjectContainer oc = (ObjectContainer) c;
                    value = oc.external ? getObjectFromFile(oc, cipher) : ((ObjectContainer) c).value;
                    break;
            }
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    private synchronized void notifyListeners(String key) {
        if (listeners.isEmpty()) return;
        for (SharedPreferences.OnSharedPreferenceChangeListener listener : listeners) {
            mainHandler.post(() -> listener.onSharedPreferenceChanged(this, key));
        }
    }

    @Override
    public synchronized void registerOnSharedPreferenceChangeListener(
            OnSharedPreferenceChangeListener listener) {
        if (listener == null) {
            return;
        }
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    @Override
    public synchronized void unregisterOnSharedPreferenceChangeListener(
            OnSharedPreferenceChangeListener listener) {
        listeners.remove(listener);
    }

    // Put methods
    public synchronized Editor putBoolean(String key, boolean value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.BOOLEAN) {
            remove(key);
            container = null;
        }
        BooleanContainer c = (BooleanContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.BOOLEAN)) return this;
            int offset = fastBuffer.position;
            fastBuffer.put((byte) (value ? 1 : 0));
            updateChange();
            data.put(key, new BooleanContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            c.value = value;
            updateBoolean((byte) (value ? 1 : 0), c.offset);
            handleChange(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putInt(String key, int value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.INT) {
            remove(key);
            container = null;
        }
        IntContainer c = (IntContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.INT)) return this;
            int offset = fastBuffer.position;
            fastBuffer.putInt(cipher != null ? cipher.encrypt(value) : value);
            updateChange();
            data.put(key, new IntContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            int newValue = cipher != null ? cipher.encrypt(value) : value;
            int oldValue = cipher != null ? fastBuffer.getInt(c.offset) : c.value;
            long sum = (newValue ^ oldValue) & 0xFFFFFFFFL;
            c.value = value;
            updateInt32(newValue, sum, c.offset);
            handleChange(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putFloat(String key, float value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.FLOAT) {
            remove(key);
            container = null;
        }
        FloatContainer c = (FloatContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.FLOAT)) return this;
            int offset = fastBuffer.position;
            fastBuffer.putInt(getNewFloatValue(value));
            updateChange();
            data.put(key, new FloatContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            int newValue = getNewFloatValue(value);
            int oldValue = fastBuffer.getInt(c.offset);
            long sum = (newValue ^ oldValue) & 0xFFFFFFFFL;
            c.value = value;
            updateInt32(newValue, sum, c.offset);
            handleChange(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putLong(String key, long value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.LONG) {
            remove(key);
            container = null;
        }
        LongContainer c = (LongContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.LONG)) return this;
            int offset = fastBuffer.position;
            fastBuffer.putLong(cipher != null ? cipher.encrypt(value) : value);
            updateChange();
            data.put(key, new LongContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            long newValue = cipher != null ? cipher.encrypt(value) : value;
            long oldValue = cipher != null ? fastBuffer.getLong(c.offset) : c.value;
            long sum = newValue ^ oldValue;
            c.value = value;
            updateInt64(newValue, sum, c.offset);
            handleChange(key);
        }
        return this;
    }

    public synchronized Editor putDouble(String key, double value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.DOUBLE) {
            remove(key);
            container = null;
        }
        DoubleContainer c = (DoubleContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.DOUBLE)) return this;
            int offset = fastBuffer.position;
            fastBuffer.putLong(getNewDoubleValue(value));
            updateChange();
            data.put(key, new DoubleContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            long newValue = getNewDoubleValue(value);
            long oldValue = fastBuffer.getLong(c.offset);
            long sum = newValue ^ oldValue;
            c.value = value;
            updateInt64(newValue, sum, c.offset);
            handleChange(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putString(String key, String value) {
        if (closed) return this;
        checkKey(key);
        if (value == null) {
            remove(key);
        } else {
            BaseContainer container = data.get(key);
            if (container != null && container.getType() != DataType.STRING) {
                remove(key);
                container = null;
            }
            StringContainer c = (StringContainer) container;
            if (c != null && !c.external && value.equals(c.value)) {
                return this;
            }
            byte[] bytes = value.isEmpty() ? EMPTY_ARRAY : value.getBytes(StandardCharsets.UTF_8);
            byte[] newBytes = cipher != null ? cipher.encrypt(bytes) : bytes;
            if (newBytes == null) {
                error(new Exception(ENCRYPT_FAILED));
                return this;
            }
            addOrUpdate(key, value, newBytes, c, DataType.STRING);
            handleChange(key);
        }
        return this;
    }

    public synchronized Editor putArray(String key, byte[] value) {
        if (closed) return this;
        checkKey(key);
        if (value == null) {
            remove(key);
        } else {
            BaseContainer container = data.get(key);
            if (container != null && container.getType() != DataType.ARRAY) {
                remove(key);
                container = null;
            }
            ArrayContainer c = (ArrayContainer) container;
            byte[] newBytes = cipher != null ? cipher.encrypt(value) : value;
            if (newBytes == null) {
                error(new Exception(ENCRYPT_FAILED));
                return this;
            }
            addOrUpdate(key, value, newBytes, c, DataType.ARRAY);
            handleChange(key);
        }
        return this;
    }

    /**
     * @param key     The name of the data to modify
     * @param value   The new value
     * @param encoder The encoder to encode value to byte[], encoder must register in  Builder.encoder(),
     *                for decoding byte[] to object in next loading.
     * @param <T>     Type of value
     */
    public synchronized <T> void putObject(String key, T value, FastEncoder<T> encoder) {
        if (closed) return;
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
        byte[] objBytes = null;
        try {
            objBytes = encoder.encode(value);
        } catch (Exception e) {
            error(e);
        }
        if (objBytes == null) {
            remove(key);
            return;
        }

        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.OBJECT) {
            remove(key);
            container = null;
        }
        ObjectContainer c = (ObjectContainer) container;

        // assemble object bytes
        int tagSize = FastBuffer.getStringSize(tag);
        FastBuffer buffer = new FastBuffer(1 + tagSize + objBytes.length);
        buffer.put((byte) tagSize);
        buffer.putString(tag);
        buffer.putBytes(objBytes);
        byte[] bytes = buffer.hb;

        byte[] newBytes = cipher != null ? cipher.encrypt(bytes) : bytes;
        if (newBytes == null) return;
        addOrUpdate(key, value, newBytes, c, DataType.OBJECT);
        handleChange(key);
    }

    public synchronized Editor putStringSet(String key, Set<String> set) {
        if (closed) return this;
        if (set == null) {
            remove(key);
        } else {
            putObject(key, set, StringSetEncoder.INSTANCE);
        }
        return this;
    }

    // Helper methods for put operations
    private void preparePutBytes() {
        ensureSize(updateSize);
        updateStart = dataEnd;
        dataEnd += updateSize;
        fastBuffer.position = updateStart;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean wrapHeader(String key, byte type) {
        return wrapHeader(key, type, TYPE_SIZE[type]);
    }

    private boolean wrapHeader(String key, byte type, int valueSize) {
        if (cipher != null) {
            byte[] keyBytes = cipher.encrypt(key.getBytes(StandardCharsets.UTF_8));
            if (keyBytes == null) {
                error(new Exception(ENCRYPT_FAILED));
                return false;
            }
            int keySize = keyBytes.length;
            prepareHeaderInfo(keySize, valueSize, type);
            fastBuffer.put((byte) keySize);
            System.arraycopy(keyBytes, 0, fastBuffer.hb, fastBuffer.position, keySize);
            fastBuffer.position += keySize;
        } else {
            int keySize = FastBuffer.getStringSize(key);
            prepareHeaderInfo(keySize, valueSize, type);
            wrapKey(key, keySize);
        }
        return true;
    }

    private void prepareHeaderInfo(int keySize, int valueSize, byte type) {
        if (keySize > 0xFF) {
            throw new IllegalArgumentException("key's length must less than 256");
        }
        updateSize = 2 + keySize + valueSize;
        preparePutBytes();
        fastBuffer.put(type);
    }

    private void wrapKey(String key, int keySize) {
        fastBuffer.put((byte) keySize);
        if (keySize == key.length()) {
            //noinspection deprecation
            key.getBytes(0, keySize, fastBuffer.hb, fastBuffer.position);
            fastBuffer.position += keySize;
        } else {
            fastBuffer.putString(key);
        }
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
    }

    private void addObject(String key, Object value, byte[] bytes, byte type) {
        int offset = saveArray(key, bytes, type);
        if (offset > 0) {
            int size = bytes.length;
            BaseContainer c;
            if (type == DataType.STRING) {
                c = new StringContainer(updateStart, offset, (String) value, size, false);
            } else if (type == DataType.ARRAY) {
                c = new ArrayContainer(updateStart, offset, value, size, false);
            } else {
                c = new ObjectContainer(updateStart, offset, value, size, false);
            }
            data.put(key, c);
            updateChange();
        }
    }

    private void updateObject(String key, Object value, byte[] bytes, VarContainer c) {
        int offset = saveArray(key, bytes, c.getType());
        if (offset > 0) {
            String oldFileName = c.external ? (String) c.value : null;
            remove(c.getType(), c.start, c.offset + c.valueSize);
            c.start = updateStart;
            c.offset = offset;
            c.external = false;
            c.value = value;
            c.valueSize = bytes.length;
            updateChange();
            checkGC();
            if (oldFileName != null) {
                removeOldFile(oldFileName);
            }
        }
    }

    /**
     * Return offset when saving success;
     * Return 0 when saving failed.
     */
    private int saveArray(String key, byte[] value, byte type) {
        return wrapArray(key, value, type);
    }

    private int wrapArray(String key, byte[] value, byte type) {
        if (!wrapHeader(key, type, 2 + value.length)) {
            return 0;
        }
        fastBuffer.putShort((short) value.length);
        int offset = fastBuffer.position;
        fastBuffer.putBytes(value);
        return offset;
    }

    private int getNewFloatValue(float value) {
        int intValue = Float.floatToRawIntBits(value);
        return cipher != null ? cipher.encrypt(intValue) : intValue;
    }

    private long getNewDoubleValue(double value) {
        long longValue = Double.doubleToRawLongBits(value);
        return cipher != null ? cipher.encrypt(longValue) : longValue;
    }

    /**
     * Close the kv instance. <br>
     * If the kv closed, it will not accept any updates.<br>
     * If the kv is cached, don't forget to remove it from cache once you call this method.
     */
    public synchronized void close() {
        if (closed) return;
        closed = true;
        if (writingMode == NON_BLOCKING) {
            try {
                aChannel.force(true);
                aChannel.close();
                bChannel.force(true);
                bChannel.close();
            } catch (Exception e) {
                error(e);
            }
        }
        synchronized (Builder.class) {
            Builder.INSTANCE_MAP.remove(path + name);
        }
    }

    public static final class Builder {
        static final Map<String, FastKV> INSTANCE_MAP = new ConcurrentHashMap<>();
        private final String path;
        private final String name;
        private FastEncoder[] encoders;
        private FastCipher cipher;
        private int writingMode = NON_BLOCKING;

        public Builder(Context context, String name) {
            if (context == null) {
                throw new IllegalArgumentException("context is null");
            }
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name is empty");
            }
            this.path = context.getFilesDir().getAbsolutePath() + "/fastkv/";
            this.name = name;
        }

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
        public Builder encoder(FastEncoder[] encoders) {
            this.encoders = encoders;
            return this;
        }

        /**
         * Set encryption cipher.
         */
        public Builder cipher(FastCipher cipher) {
            this.cipher = cipher;
            return this;
        }

        /**
         * Assigned writing mode to SYNC_BLOCKING.
         * <p>
         * In non-blocking mode (write data with mmap),
         * it might lost update if the system crash or power off before flush data to disk.
         * You could use {@link #force()} to avoid losing update, or use SYNC_BLOCKING mode.
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
                        kv = new FastKV(path, name, encoders, cipher, writingMode);
                        INSTANCE_MAP.put(key, kv);
                    }
                }
            }
            return kv;
        }
    }

    /**
     * Adapt old SharePreferences,
     * return a new SharedPreferences with storage strategy of FastKV.
     * <p>
     * Node: The old SharePreferences must implement getAll() method,
     * otherwise can not import old data to new files.
     *
     * @param context       The context
     * @param name          The name of SharePreferences
     * @return The Wrapper of FastKV, which implement SharePreferences.
     */
    public static SharedPreferences adapt(Context context, String name) {
        String path = context.getFilesDir().getAbsolutePath() + "/fastkv";
        FastKV kv = new FastKV.Builder(path, name).build();
        final String flag = "kv_import_flag";
        if (!kv.contains(flag)) {
            SharedPreferences oldPreferences = context.getSharedPreferences(name, Context.MODE_PRIVATE);
            //noinspection unchecked
            Map<String, Object> allData = (Map<String, Object>) oldPreferences.getAll();
            kv.putAll(allData);
            kv.putBoolean(flag, true);
        }
        return kv;
    }

    @NonNull
    @Override
    public String toString() {
        return "FastKV: path:" + path + " name:" + name;
    }
}
