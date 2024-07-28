package io.fastkv;

import android.content.Context;
import android.content.SharedPreferences;

import androidx.annotation.NonNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import io.fastkv.Container.BaseContainer;
import io.fastkv.Container.VarContainer;
import io.fastkv.interfaces.FastCipher;
import io.fastkv.interfaces.FastEncoder;

/**
 * FastKV has three writing mode: <br>
 * non-blocking: write partial data with mmap). <br>
 * async-block: write all data to disk with blocking I/O asynchronously, likes 'apply' of  SharePreferences.<br>
 * sync-block: write all data too, but it likes 'commit' of  SharePreferences.<br>
 * FastKV is not support multi-process, use {@link MPFastKV} if you want to support multi-process.
 * <br>
 * Note: <br>
 * 1. Do not change file name once create. <br>
 * 2. Do not change cipher once create.
 *    But it's okay to apply cipher from the state of no cipher.<br>
 * 3. Do not change value type for one key.<br>
 */
@SuppressWarnings("rawtypes")
public final class FastKV extends AbsFastKV {
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

    private final Executor applyExecutor = new LimitExecutor();

    FastKV(final String path,
           final String name,
           FastEncoder[] encoders,
           FastCipher cipher,
           int writingMode) {
        super(path, name, encoders, cipher);
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

    protected void copyToMainFile(FastKV tempKV) {
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

    @SuppressWarnings("resource")
    private boolean writeToABFile(FastBuffer buffer) {
        int fileLen = buffer.hb.length;
        File aFile = new File(path, name + A_SUFFIX);
        File bFile = new File(path, name + B_SUFFIX);
        try {
            if (!Utils.makeFileIfNotExist(aFile) || !Utils.makeFileIfNotExist(bFile)) {
                throw new Exception(OPEN_FILE_FAILED);
            }
            RandomAccessFile aAccessFile = new RandomAccessFile(aFile, "rw");
            aAccessFile.setLength(fileLen);
            aChannel = aAccessFile.getChannel();
            aBuffer = aChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            aBuffer.order(ByteOrder.LITTLE_ENDIAN);
            aBuffer.put(buffer.hb, 0, dataEnd);

            RandomAccessFile bAccessFile = new RandomAccessFile(bFile, "rw");
            bAccessFile.setLength(fileLen);
            bChannel = bAccessFile.getChannel();
            bBuffer = bChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            bBuffer.order(ByteOrder.LITTLE_ENDIAN);
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

    public synchronized Editor remove(String key) {
        if (closed) return this;
        BaseContainer container = data.get(key);
        if (container != null) {
            final String oldFileName;
            data.remove(key);
            bigValueCache.remove(key);
            externalCache.remove(key);
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
                    FastKVConfig.getExecutor().execute(() -> Utils.deleteFile(new File(path + name, oldFileName)));
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
        super.putAll(values, encoders);
        if (writingMode != NON_BLOCKING) {
            commit();
        }
    }

    /**
     * Forces any changes to be written to the storage device containing the mapped file.
     * No need to call this unless what's had written is very import.
     * The system crash or power off before data syncing to disk might make recently update loss.
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

    @Override
    protected void handleChange(String key) {
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
            if (Utils.saveBytes(tmpFile, fastBuffer.hb, dataEnd)) {
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
                FastKVConfig.getExecutor().execute(() -> Utils.deleteFile(new File(path + name, oldFileName)));
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

    protected void updateChange() {
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

    protected void ensureSize(int allocate) {
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

    protected void updateBoolean(byte value, int offset) {
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

    protected void updateInt32(int value, long sum, int offset) {
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

    protected void updateInt64(long value, long sum, int offset) {
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

    protected void updateBytes(int offset, byte[] bytes) {
        super.updateBytes(offset, bytes);
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

    protected void removeOldFile(String oldFileName) {
        if (writingMode == NON_BLOCKING) {
            FastKVConfig.getExecutor().execute(() -> Utils.deleteFile(new File(path + name, oldFileName)));
        } else {
            deletedFiles.add(oldFileName);
        }
    }

    protected void remove(byte type, int start, int end) {
        super.remove(type, start, end);
        removeStart = start;
    }

    protected void checkGC() {
        if (invalidBytes >= (bytesThreshold() << 1)
                || invalids.size() >= (dataEnd < (1 << 14) ? BASE_GC_KEYS_THRESHOLD : BASE_GC_KEYS_THRESHOLD << 1)) {
            gc(0);
        }
    }

    protected void syncCompatBuffer(int gcStart, int allocate, int gcUpdateSize) {
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
                aChannel.force(false);
                aChannel.close();
                bChannel.force(false);
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

    @Override
    public synchronized @NonNull
    String toString() {
        return "FastKV: path:" + path + " name:" + name;
    }
}
