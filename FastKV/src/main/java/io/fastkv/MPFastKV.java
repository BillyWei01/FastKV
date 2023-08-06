package io.fastkv;

import android.os.FileObserver;
import android.os.Handler;
import android.os.Looper;
import android.os.Message;

import androidx.annotation.NonNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import io.fastkv.Container.BaseContainer;
import io.fastkv.Container.VarContainer;
import io.fastkv.interfaces.FastCipher;
import io.fastkv.interfaces.FastEncoder;

/**
 * Multi-process FastKV <br>
 * <p>
 * This class support cross process storage and update notify.
 * It implements API of SharePreferences, so it could be used as SharePreferences.<br>
 * <p>
 * Note 1: <br>
 * Remember to call 'commit' or 'apply' after editing !!!
 * <p>
 * Note 2: <br>
 * To support cross process storage, MPFastKV needs to do many state checks, it's slower than {@link FastKV}.
 * So if you don't need to access the data in multi-process, just use FastKV.
 */
@SuppressWarnings("rawtypes")
public final class MPFastKV extends AbsFastKV {
    private static final int MSG_REFRESH = 1;
    private static final int MSG_APPLY = 2;
    private static final int MSG_DATA_CHANGE = 3;
    private static final int MSG_CLEAR = 4;
    private static final int LOCK_TIMEOUT = 3000;

    private static final Random random = new Random();

    private final boolean needWatchFileChange;
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

    private long updateHash;
    private boolean needFullWrite = false;

    private final Executor applyExecutor = new LimitExecutor();
    private final Executor refreshExecutor = new LimitExecutor();

    // We need to keep reference to the observer in case of gc recycle it.
    private volatile KVFileObserver fileObserver;

    private final Set<String> changedKey = new HashSet<>();

    MPFastKV(final String path,
             final String name,
             FastEncoder[] encoders,
             FastCipher cipher,
             boolean needWatchFileChange) {
        super(path, name, encoders, cipher);
        aFile = new File(path, name + A_SUFFIX);
        bFile = new File(path, name + B_SUFFIX);
        this.needWatchFileChange = needWatchFileChange;

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
        if (!loadFromCFile()) {
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
        trySettingObserver();
    }

    protected void copyToMainFile(FastKV tempKV) {
        writeToABFile(tempKV.fastBuffer);
    }

    private void trySettingObserver() {
        if (needWatchFileChange && fileObserver == null && bFile != null && bFile.exists()) {
            fileObserver = new KVFileObserver(bFile.getPath());
            fileObserver.startWatching();
        }
    }

    private void loadFromABFile() {
        try {
            int count = 0;
            while ((!Utils.makeFileIfNotExist(aFile) || !Utils.makeFileIfNotExist(bFile)) && count < 3) {
                //noinspection BusyWait
                Thread.sleep(20L);
                count++;
            }
            if (!aFile.exists() || !bFile.exists()) {
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
                        int aSize = aBuffer.getInt();
                        int aDataSize = unpackSize(aSize);
                        boolean aHadEncrypted = isCipher(aSize);
                        boolean isAValid = false;
                        if (aDataSize >= 0 && (aDataSize <= aFileLen - DATA_START)) {
                            dataEnd = DATA_START + aDataSize;
                            long aCheckSum = aBuffer.getLong(4);
                            aBuffer.rewind();
                            aBuffer.get(fastBuffer.hb, 0, dataEnd);
                            if (aCheckSum == fastBuffer.getChecksum(DATA_START, aDataSize) && parseData(aHadEncrypted)) {
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
            if (!Utils.makeFileIfNotExist(bFile)) {
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
                if (!Utils.makeFileIfNotExist(aFile)) {
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
        } catch (Exception e) {
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
            if (!Utils.makeFileIfNotExist(aFile) || !Utils.makeFileIfNotExist(bFile)) {
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
            trySettingObserver();
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

    public synchronized Editor remove(String key) {
        lockAndCheckUpdate();
        handleChange(key);
        BaseContainer container = data.get(key);
        if (container != null) {
            String oldFileName = null;
            data.remove(key);
            bigValueCache.remove(key);
            externalCache.remove(key);
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

    @Override
    protected void handleChange(String key) {
        if (!listeners.isEmpty()) {
            changedKey.add(key);
        }
    }

    private synchronized void notifyChangedKeys() {
        if (!changedKey.isEmpty()) {
            for (String key : changedKey) {
                notifyListeners(key);
            }
            changedKey.clear();
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
        int dataSize = unpackSize(fastBuffer.getInt());
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
            int packedSize = packSize(dataSize);
            fastBuffer.putInt(0, packedSize);
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
            // syncBufferToA(0, DATA_START);
            aBuffer.putInt(0, packedSize);
            aBuffer.putLong(4, checksum);
            for (int i = 0; i < updateCount; i += 2) {
                int start = updateStartAndSize[i];
                int size = updateStartAndSize[i + 1];
                syncBufferToA(start, size);
            }
            if (dataEnd + 8 < aBuffer.capacity()) {
                updateHash = random.nextLong() ^ System.nanoTime();
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
                    FastKVConfig.getExecutor().execute(() -> Utils.deleteFile(new File(path + name, oldFileName)));
                }
            }

            if (fastBuffer.hb.length - dataEnd > TRUNCATE_THRESHOLD) {
                truncate();
            }

            return true;
        } catch (Exception e) {
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

    protected void lockAndCheckUpdate() {
        if (bFileLock != null) {
            return;
        }
        if (bChannel == null) {
            loadFromABFile();
            trySettingObserver();
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
            } catch (Exception e) {
                error(e);
            }
        }
    }

    private synchronized void releaseLock() {
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
        int size = fastBuffer.getInt();
        int dataSize = unpackSize(size);
        boolean hadEncrypted = isCipher(size);
        checksum = fastBuffer.getLong();
        dataEnd = DATA_START + dataSize;
        if (checksum != fastBuffer.getChecksum(DATA_START, dataSize) || !parseData(hadEncrypted)) {
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
        int size = buffer.getInt(0);
        int dataSize = unpackSize(size);
        boolean hadEncrypted = isCipher(size);
        if (dataSize < 0 || dataSize > capacity) {
            throw new IllegalStateException("Invalid file, dataSize:" + dataSize + ", capacity:" + capacity);
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
            if (sum == fastBuffer.getChecksum(DATA_START, dataSize) && parseData(hadEncrypted)) {
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
            aBuffer.putInt(0, packSize(0));
            aBuffer.putLong(4, 0L);
            getUpdateHash();
            if (Utils.makeFileIfNotExist(bFile)) {
                setBFileSize(PAGE_SIZE);
                syncAToB(0, DATA_START);
                trySettingObserver();
            }
        } catch (Exception e) {
            error(e);
            needFullWrite = true;
        }
        Utils.deleteFile(new File(path + name));
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

    protected void updateChange() {
        checksum ^= fastBuffer.getChecksum(updateStart, updateSize);
        if (updateSize != 0) {
            addUpdate(updateStart, updateSize);
            updateSize = 0;
        }
    }

    protected void ensureSize(int allocate) {
        int capacity = fastBuffer.hb.length;
        int expected = dataEnd + allocate + 8;
        if (expected >= capacity) {
            int newCapacity = getNewCapacity(capacity, expected);
            byte[] bytes = new byte[newCapacity];
            System.arraycopy(fastBuffer.hb, 0, bytes, 0, dataEnd);
            fastBuffer.hb = bytes;
        }
    }

    protected void updateBoolean(byte value, int offset) {
        checksum ^= shiftCheckSum(1L, offset);
        fastBuffer.hb[offset] = value;
        addUpdate(offset, 1);
    }

    protected void updateInt32(int value, long sum, int offset) {
        checksum ^= shiftCheckSum(sum, offset);
        fastBuffer.putInt(offset, value);
        addUpdate(offset, 4);
    }

    protected void updateInt64(long value, long sum, int offset) {
        checksum ^= shiftCheckSum(sum, offset);
        fastBuffer.putLong(offset, value);
        addUpdate(offset, 8);
    }

    protected void updateBytes(int offset, byte[] bytes) {
        super.updateBytes(offset, bytes);
        addUpdate(offset, bytes.length);
    }

    protected void removeOldFile(String oldFileName) {
        deletedFiles.add(oldFileName);
    }

    protected void remove(byte type, int start, int end) {
        super.remove(type, start, end);
        addUpdate(start, 1);
    }

    protected void checkGC() {
        if (invalidBytes >= bytesThreshold() || invalids.size() >= BASE_GC_KEYS_THRESHOLD) {
            gc(0);
        }
    }

    protected void syncCompatBuffer(int gcStart, int allocate, int gcUpdateSize) {
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
                    notifyListeners(null);
                    break;
                default:
                    break;
            }
        }
    };

    private class KVFileObserver extends FileObserver {
        public KVFileObserver(String path) {
            super(path, FileObserver.MODIFY);
        }

        @Override
        public void onEvent(int event, String path) {
            // Delay a few time to avoid frequency refresh.
            if (!kvHandler.hasMessages(MSG_REFRESH)) {
                kvHandler.sendEmptyMessageDelayed(MSG_REFRESH, 30L);
            }
        }
    }

    public static class Builder {
        private static final Map<String, MPFastKV> INSTANCE_MAP = new ConcurrentHashMap<>();
        private final String path;
        private final String name;
        private FastEncoder[] encoders;
        private FastCipher cipher;
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
                        kv = new MPFastKV(path, name, encoders, cipher, needWatchFileChange);
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
