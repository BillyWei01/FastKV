package io.fastkv;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fastkv.Container.*;
import io.fastkv.interfaces.FastCipher;
import io.fastkv.interfaces.FastEncoder;

/**
 * 文件I/O辅助类，使用扩展方法模式处理FastKV的文件操作。
 */
class FileHelper {
    static final String BOTH_FILES_ERROR = "both files error";
    private static final String OPEN_FILE_FAILED = "open file failed";
    static final String MAP_FAILED = "map failed";

    static final String A_SUFFIX = ".kva";
    static final String B_SUFFIX = ".kvb";
    static final String C_SUFFIX = ".kvc";
    static final String TEMP_SUFFIX = ".tmp";

    private static final int DATA_SIZE_LIMIT = 1 << 28; // 256M

    /**
     * 加载A/B文件
     * 
     * @param kv FastKV实例
     */
    @SuppressWarnings("resource")
    static void loadFromABFile(FastKV kv) {
        File aFile = new File(kv.path, kv.name + A_SUFFIX);
        File bFile = new File(kv.path, kv.name + B_SUFFIX);
        try {
            if (!Utils.makeFileIfNotExist(aFile) || !Utils.makeFileIfNotExist(bFile)) {
                LoggerHelper.error(kv, new Exception(OPEN_FILE_FAILED));
                toBlockingMode(kv);
                return;
            }
            RandomAccessFile aAccessFile = new RandomAccessFile(aFile, "rw");
            RandomAccessFile bAccessFile = new RandomAccessFile(bFile, "rw");
            long aFileLen = aAccessFile.length();
            long bFileLen = bAccessFile.length();
            kv.aChannel = aAccessFile.getChannel();
            kv.bChannel = bAccessFile.getChannel();
            try {
                kv.aBuffer = kv.aChannel.map(FileChannel.MapMode.READ_WRITE, 0, 
                        aFileLen > 0 ? aFileLen : FastKV.PAGE_SIZE);
                kv.aBuffer.order(ByteOrder.LITTLE_ENDIAN);
                kv.bBuffer = kv.bChannel.map(FileChannel.MapMode.READ_WRITE, 0, 
                        bFileLen > 0 ? bFileLen : FastKV.PAGE_SIZE);
                kv.bBuffer.order(ByteOrder.LITTLE_ENDIAN);
            } catch (IOException e) {
                LoggerHelper.error(kv, e);
                toBlockingMode(kv);
                tryBlockingIO(kv, aFile, bFile);
                return;
            }
            kv.fastBuffer = new FastBuffer(kv.aBuffer.capacity());

            if (aFileLen == 0 && bFileLen == 0) {
                kv.dataEnd = FastKV.DATA_START;
            } else {
                processExistingFiles(kv, aFileLen, bFileLen);
            }
        } catch (Exception e) {
            LoggerHelper.error(kv, e);
            resetMemory(kv);
            toBlockingMode(kv);
        }
    }
    
    /**
     * 处理已存在的A/B文件数据
     */
    private static void processExistingFiles(FastKV kv, long aFileLen, long bFileLen) {
        int aSize = kv.aBuffer.getInt();
        int aDataSize = BufferHelper.unpackSize(aSize);
        boolean aHadEncrypted = BufferHelper.isCipher(aSize);
        long aCheckSum = kv.aBuffer.getLong();

        int bSize = kv.bBuffer.getInt();
        int bDataSize = BufferHelper.unpackSize(bSize);
        boolean bHadEncrypted = BufferHelper.isCipher(bSize);
        long bCheckSum = kv.bBuffer.getLong();

        boolean isAValid = false;
        if (aDataSize >= 0 && (aDataSize <= aFileLen - FastKV.DATA_START)) {
            kv.dataEnd = FastKV.DATA_START + aDataSize;
            kv.aBuffer.rewind();
            kv.aBuffer.get(kv.fastBuffer.hb, 0, kv.dataEnd);
            if (aCheckSum == kv.fastBuffer.getChecksum(FastKV.DATA_START, aDataSize) && DataParser.parseData(kv, aHadEncrypted)) {
                kv.checksum = aCheckSum;
                isAValid = true;
            }
        }
        if (isAValid) {
            if (aFileLen != bFileLen || !isABFileEqual(kv)) {
                LoggerHelper.warning(kv, new Exception("B file error"));
                copyBuffer(kv, kv.aBuffer, kv.bBuffer, kv.dataEnd);
            }
        } else {
            processFileB(kv, bFileLen, bDataSize, bCheckSum, bHadEncrypted);
        }
    }

    /**
     * 处理B文件数据
     */
    private static void processFileB(FastKV kv, long bFileLen, int bDataSize, long bCheckSum, boolean bHadEncrypted)  {
        boolean isBValid = false;
        if (bDataSize >= 0 && (bDataSize <= bFileLen - FastKV.DATA_START)) {
            kv.data.clear();
            GCHelper.clearInvalid(kv);
            kv.dataEnd = FastKV.DATA_START + bDataSize;
            if (kv.fastBuffer.hb.length != kv.bBuffer.capacity()) {
                kv.fastBuffer = new FastBuffer(kv.bBuffer.capacity());
            }
            kv.bBuffer.rewind();
            kv.bBuffer.get(kv.fastBuffer.hb, 0, kv.dataEnd);
            if (bCheckSum == kv.fastBuffer.getChecksum(FastKV.DATA_START, bDataSize) && DataParser.parseData(kv, bHadEncrypted)) {
                LoggerHelper.warning(kv, new Exception("A file error"));
                copyBuffer(kv, kv.bBuffer, kv.aBuffer, kv.dataEnd);
                kv.checksum = bCheckSum;
                isBValid = true;
            }
        }
        if (!isBValid) {
            LoggerHelper.error(kv, BOTH_FILES_ERROR);
            clearData(kv);
        }
    }

    /**
     * 写入数据到A/B文件
     * 
     * @param kv FastKV实例
     * @param buffer 数据缓冲区
     * @return 是否成功
     */
    static boolean writeToABFile(FastKV kv, FastBuffer buffer) {
        RandomAccessFile aAccessFile = null;
        RandomAccessFile bAccessFile = null;
        try {
            int fileLen = buffer.hb.length;
            File aFile = new File(kv.path, kv.name + A_SUFFIX);
            File bFile = new File(kv.path, kv.name + B_SUFFIX);
            if (!Utils.makeFileIfNotExist(aFile) || !Utils.makeFileIfNotExist(bFile)) {
                throw new Exception(OPEN_FILE_FAILED);
            }
            aAccessFile = new RandomAccessFile(aFile, "rw");
            aAccessFile.setLength(fileLen);
            kv.aChannel = aAccessFile.getChannel();
            kv.aBuffer = kv.aChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            kv.aBuffer.order(ByteOrder.LITTLE_ENDIAN);
            kv.aBuffer.put(buffer.hb, 0, kv.dataEnd);

            bAccessFile = new RandomAccessFile(bFile, "rw");
            bAccessFile.setLength(fileLen);
            kv.bChannel = bAccessFile.getChannel();
            kv.bBuffer = kv.bChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            kv.bBuffer.order(ByteOrder.LITTLE_ENDIAN);
            kv.bBuffer.put(buffer.hb, 0, kv.dataEnd);
            return true;
        } catch (Exception e) {
            Utils.closeQuietly(aAccessFile);
            Utils.closeQuietly(bAccessFile);
            kv.aChannel = null;
            kv.bChannel = null;
            kv.aBuffer = null;
            kv.bBuffer = null;
            LoggerHelper.error(kv, e);
        }
        return false;
    }
    
    /**
     * 检查A/B文件是否相等
     */
    static boolean isABFileEqual(FastKV kv) {
        FastBuffer tempBuffer = new FastBuffer(kv.dataEnd);
        kv.bBuffer.rewind();
        kv.bBuffer.get(tempBuffer.hb, 0, kv.dataEnd);
        byte[] a = kv.fastBuffer.hb;
        byte[] b = tempBuffer.hb;
        for (int i = 0; i < kv.dataEnd; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * 复制缓冲区数据
     * 
     * @param kv FastKV实例
     * @param src 源缓冲区
     * @param dest 目标缓冲区
     * @param end 复制结束位置
     */
    static void copyBuffer(FastKV kv, MappedByteBuffer src, MappedByteBuffer dest, int end) {
        if (src.capacity() != dest.capacity()) {
            FileChannel channel = (dest == kv.bBuffer) ? kv.bChannel : kv.aChannel;
            MappedByteBuffer newBuffer = remapBuffer(channel, src.capacity());
            if (newBuffer == null) {
                LoggerHelper.error(kv, new Exception(MAP_FAILED));
                toBlockingMode(kv);
                return;
            }
            if (dest == kv.bBuffer) {
                kv.bBuffer = newBuffer;
            } else {
                kv.aBuffer = newBuffer;
            }
            dest = newBuffer;
        }
        src.rewind();
        dest.rewind();
        src.limit(end);
        dest.put(src);
        src.limit(src.capacity());
    }

    /**
     * 重新映射缓冲区到新的容量
     */
    static MappedByteBuffer remapBuffer(FileChannel channel, int newCapacity) {
        try {
            MappedByteBuffer newBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
            newBuffer.order(ByteOrder.LITTLE_ENDIAN);
            return newBuffer;
        } catch (IOException e) {
            return null;
        }
    }
    
    /**
     * 写入数据到C文件
     * 
     * @param kv FastKV实例
     * @return 是否成功
     */
    static boolean writeToCFile(FastKV kv) {
        try {
            File tmpFile = new File(kv.path, kv.name + TEMP_SUFFIX);
            if (Utils.makeFileIfNotExist(tmpFile)) {
                try (RandomAccessFile accessFile = new RandomAccessFile(tmpFile, "rw")) {
                    accessFile.setLength(kv.dataEnd);
                    accessFile.write(kv.fastBuffer.hb, 0, kv.dataEnd);
                    accessFile.getFD().sync();
                }
                File cFile = new File(kv.path, kv.name + C_SUFFIX);
                if (Utils.renameFile(tmpFile, cFile)) {
                    clearDeletedFiles(kv);
                    return true;
                } else {
                    LoggerHelper.warning(kv, new Exception("rename failed"));
                }
            }
        } catch (Exception e) {
            LoggerHelper.error(kv, e);
        }
        return false;
    }

    /**
     * 强制同步
     * 
     * @param kv FastKV实例
     */
    static void force(FastKV kv) {
        if (kv.closed) return;
        if (kv.writingMode == FastKV.NON_BLOCKING) {
            forceBuffer(kv.aBuffer);
            forceBuffer(kv.bBuffer);
        }
    }

    /**
     * 关闭文件
     * 
     * @param kv FastKV实例
     */
    static void close(FastKV kv) {
        if (kv.closed) return;
        kv.closed = true;
        if (kv.writingMode == FastKV.NON_BLOCKING) {
            forceChannel(kv.aChannel);
            closeChannel(kv.aChannel);
            forceChannel(kv.bChannel);
            closeChannel(kv.bChannel);
        }
    }
    
    // ==================== 辅助方法 ====================
    
    /**
     * 强制同步缓冲区到磁盘
     */
    private static void forceBuffer(MappedByteBuffer buffer) {
        if (buffer != null) {
            buffer.force();
        }
    }
    
    /**
     * 强制同步文件通道到磁盘
     */
    private static void forceChannel(FileChannel channel) {
        if (channel != null) {
            try {
                channel.force(true);
            } catch (IOException e) {
                // 忽略异常
            }
        }
    }
    
    /**
     * 关闭文件通道
     */
    private static void closeChannel(FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                // 忽略异常
            }
        }
    }

    /**
     * 转换为阻塞模式
     */
    static void toBlockingMode(FastKV kv) {
        kv.writingMode = FastKV.ASYNC_BLOCKING;
        Utils.closeQuietly(kv.aChannel);
        Utils.closeQuietly(kv.bChannel);
        kv.aChannel = null;
        kv.bChannel = null;
        kv.aBuffer = null;
        kv.bBuffer = null;
    }

    /**
     * 清理已删除的文件
     */
    static void clearDeletedFiles(FastKV kv) {
        if (!kv.deletedFiles.isEmpty()) {
            for (String oldFileName : kv.deletedFiles) {
                deleteExternalFile(kv, oldFileName);
            }
            kv.deletedFiles.clear();
        }
    }

    /**
     * 删除外部文件
     */
    static void deleteExternalFile(FastKV kv, String fileName) {
        FastKVConfig.getExecutor().execute(() -> Utils.deleteFile(new File(kv.path + kv.name, fileName)));
    }

    /**
     * 尝试阻塞I/O的文件模式
     */
    static void tryBlockingIO(FastKV kv, File aFile, File bFile) {
        try {
            if (loadWithBlockingIO(kv, aFile)) {
                return;
            }
        } catch (IOException e) {
            LoggerHelper.warning(kv, e);
        }
        resetMemory(kv);
        try {
            if (loadWithBlockingIO(kv, bFile)) {
                return;
            }
        } catch (IOException e) {
            LoggerHelper.warning(kv, e);
        }
        resetMemory(kv);
    }

    /**
     * 使用阻塞I/O加载文件
     */
    static boolean loadWithBlockingIO(FastKV kv, File srcFile) throws IOException {
        long fileLen = srcFile.length();
        if (fileLen == 0 || fileLen >= DATA_SIZE_LIMIT) {
            return false;
        }
        int fileSize = (int) fileLen;
        int capacity = getNewCapacity(FastKV.PAGE_SIZE, fileSize);
        FastBuffer buffer;
        if (kv.fastBuffer != null && kv.fastBuffer.hb.length == capacity) {
            buffer = kv.fastBuffer;
            buffer.position = 0;
        } else {
            buffer = new FastBuffer(new byte[capacity]);
            kv.fastBuffer = buffer;
        }
        Utils.readBytes(srcFile, buffer.hb, fileSize);
        int size = buffer.getInt();
        if (size < 0) {
            return false;
        }
        int dataSize = BufferHelper.unpackSize(size);
        boolean hadEncrypted = BufferHelper.isCipher(size);
        long sum = buffer.getLong();
        kv.dataEnd = FastKV.DATA_START + dataSize;
        if (dataSize >= 0 && (dataSize <= fileSize - FastKV.DATA_START)
                && sum == buffer.getChecksum(FastKV.DATA_START, dataSize)
                && DataParser.parseData(kv, hadEncrypted)) {
            kv.checksum = sum;
            return true;
        }
        return false;
    }

    /**
     * 从C文件加载数据（尝试）
     * 
     * @param kv FastKV实例
     * @return 是否成功写入到AB文件
     */
    static boolean loadFromCFile(FastKV kv) {
        boolean hadWriteToABFile = false;
        File cFile = new File(kv.path, kv.name + C_SUFFIX);
        File tmpFile = new File(kv.path, kv.name + TEMP_SUFFIX);
        try {
            File srcFile = null;
            if (cFile.exists()) {
                srcFile = cFile;
            } else if (tmpFile.exists()) {
                srcFile = tmpFile;
            }
            if (srcFile != null) {
                if (loadWithBlockingIO(kv, srcFile)) {
                    if (kv.writingMode == FastKV.NON_BLOCKING) {
                        if (writeToABFile(kv, kv.fastBuffer)) {
                            LoggerHelper.info(kv, "recover from c file");
                            hadWriteToABFile = true;
                            deleteCFiles(kv);
                        } else {
                            kv.writingMode = FastKV.ASYNC_BLOCKING;
                        }
                    }
                } else {
                    resetMemory(kv);
                    deleteCFiles(kv);
                }
            } else {
                // 处理以下情况：
                // 用户首先以非阻塞模式打开，然后在后续更改为阻塞模式。
                if (kv.writingMode != FastKV.NON_BLOCKING) {
                    File aFile = new File(kv.path, kv.name + A_SUFFIX);
                    File bFile = new File(kv.path, kv.name + B_SUFFIX);
                    if (aFile.exists() && bFile.exists()) {
                        tryBlockingIO(kv, aFile, bFile);
                    }
                }
            }
        } catch (Exception e) {
            LoggerHelper.error(kv, e);
        }
        return hadWriteToABFile;
    }

    /**
     * 重写数据：从未加密到加密
     * 
     * @param kv FastKV实例
     */
    static void rewrite(FastKV kv) {
        //noinspection rawtypes
        FastEncoder[] encoders = new FastEncoder[kv.encoderMap.size()];
        encoders = kv.encoderMap.values().toArray(encoders);
        String tempName = "temp_" + kv.name;

        // 这里我们使用阻塞模式的 FastKV 并关闭 'autoCommit'，
        // 使数据只保留在内存中。
        FastKV tempKV = new FastKV(kv.path, tempName, encoders, kv.cipher, FastKV.SYNC_BLOCKING);
        tempKV.autoCommit = false;

        List<String> oldExternalFiles = new ArrayList<>();
        for (Map.Entry<String, BaseContainer> entry : kv.data.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof StringContainer) {
                StringContainer c = (StringContainer) value;
                if (c.external) {
                    oldExternalFiles.add((String) c.value);
                    String bigStr = getStringFromFile(kv, c, null);
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
                    byte[] bigArray = getArrayFromFile(kv, c, null);
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
                    Object obj = getObjectFromFile(kv, c, null);
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

        // FastKV 的 'loadData()' 是在异步线程中执行的同步方法。
        // 为了确保 tempKV 加载完成，
        // 调用另一个同步方法来阻塞当前进程（如果加载未完成）。
        //noinspection ResultOfMethodCallIgnored
        tempKV.contains("");

        // 复制内存数据
        kv.fastBuffer = tempKV.fastBuffer;
        kv.checksum = tempKV.checksum;
        kv.dataEnd = tempKV.dataEnd;
        GCHelper.clearInvalid(kv);
        kv.data.clear();
        kv.data.putAll(tempKV.data);

        copyToMainFile(kv, tempKV);

        // 移动外部文件
        File tempDir = new File(kv.path, tempName);
        String currentDir = kv.path + kv.name;
        Utils.moveDirFiles(tempDir, currentDir);
        Utils.deleteFile(tempDir);
        for (String name : oldExternalFiles) {
            Utils.deleteFile(new File(currentDir, name));
        }

        kv.needRewrite = false;
    }

    /**
     * 复制数据到主文件
     * 
     * @param kv FastKV实例
     * @param tempKV 临时FastKV实例
     */
    static void copyToMainFile(FastKV kv, FastKV tempKV) {
        FastBuffer buffer = tempKV.fastBuffer;
        if (kv.writingMode == FastKV.NON_BLOCKING) {
            int capacity = buffer.hb.length;
            if (kv.aBuffer != null && kv.aBuffer.capacity() == capacity
                    && kv.bBuffer != null && kv.bBuffer.capacity() == capacity) {
                kv.aBuffer.position(0);
                kv.aBuffer.put(buffer.hb, 0, kv.dataEnd);
                kv.bBuffer.position(0);
                kv.bBuffer.put(buffer.hb, 0, kv.dataEnd);
            } else {
                if (!writeToABFile(kv, buffer)) {
                    kv.writingMode = FastKV.ASYNC_BLOCKING;
                }
            }
        }
        if (kv.writingMode != FastKV.NON_BLOCKING) {
            writeToCFile(kv);
        }
    }

    /**
     * 删除C文件
     * 
     * @param kv FastKV实例
     */
    static void deleteCFiles(FastKV kv) {
        try {
            Utils.deleteFile(new File(kv.path, kv.name + C_SUFFIX));
            Utils.deleteFile(new File(kv.path, kv.name + TEMP_SUFFIX));
        } catch (Exception e) {
            LoggerHelper.error(kv, e);
        }
    }

    /**
     * 从外部文件读取字符串
     * 
     * @param kv FastKV实例
     * @param c 字符串容器
     * @param fastCipher 加密器
     * @return 字符串内容
     */
    static String getStringFromFile(FastKV kv, StringContainer c, FastCipher fastCipher) {
        String fileName = (String) c.value;
        try {
            // 向前兼容：仍然支持读取已存在的外部文件
            byte[] bytes = Utils.getBytes(new File(kv.path + kv.name, fileName));
            if (bytes != null) {
                bytes = fastCipher != null ? fastCipher.decrypt(bytes) : bytes;
                return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
            }
        } catch (Exception e) {
            LoggerHelper.error(kv, e);
        }
        return null;
    }

    /**
     * 从外部文件读取字节数组
     * 
     * @param kv FastKV实例
     * @param c 数组容器
     * @param fastCipher 加密器
     * @return 字节数组
     */
    static byte[] getArrayFromFile(FastKV kv, ArrayContainer c, FastCipher fastCipher) {
        String fileName = (String) c.value;
        try {
            // 向前兼容：仍然支持读取已存在的外部文件
            byte[] bytes = Utils.getBytes(new File(kv.path + kv.name, fileName));
            if (bytes != null) {
                return fastCipher != null ? fastCipher.decrypt(bytes) : bytes;
            }
        } catch (Exception e) {
            LoggerHelper.error(kv, e);
        }
        return null;
    }

    /**
     * 从外部文件读取对象
     * 
     * @param kv FastKV实例
     * @param c 对象容器
     * @param fastCipher 加密器
     * @return 对象实例
     */
    static Object getObjectFromFile(FastKV kv, ObjectContainer c, FastCipher fastCipher) {
        String fileName = (String) c.value;
        try {
            // 向前兼容：仍然支持读取已存在的外部文件
            byte[] bytes = Utils.getBytes(new File(kv.path + kv.name, fileName));
            if (bytes != null) {
                bytes = fastCipher != null ? fastCipher.decrypt(bytes) : bytes;
                int tagSize = bytes[0] & 0xFF;
                String tag = kv.fastBuffer.decodeStr(bytes, 1, tagSize);
                //noinspection rawtypes
                FastEncoder encoder = kv.encoderMap.get(tag);
                if (encoder != null) {
                    c.encoder = encoder;
                    int offset = 1 + tagSize;
                    return encoder.decode(bytes, offset, bytes.length - offset);
                } else {
                    LoggerHelper.warning(kv, new Exception("No encoder for tag:" + tag));
                }
            } else {
                LoggerHelper.warning(kv, new Exception("Read object data failed"));
            }
        } catch (Exception e) {
            LoggerHelper.error(kv, e);
        }
        return null;
    }

    static int getNewCapacity(int capacity, int expected) {
        if (expected >= DATA_SIZE_LIMIT) {
            throw new IllegalStateException("data size out of limit");
        }
        if (expected <= FastKV.PAGE_SIZE) {
            return FastKV.PAGE_SIZE;
        } else {
            while (capacity < expected) {
                capacity <<= 1;
            }
            return capacity;
        }
    }

    static void clearData(FastKV kv) {
        if (kv.writingMode == FastKV.NON_BLOCKING) {
            try {
                resetBuffer(kv, kv.aBuffer);
                resetBuffer(kv, kv.bBuffer);
            } catch (Exception e) {
                toBlockingMode(kv);
            }
        }
        resetMemory(kv);
        Utils.deleteFile(new File(kv.path + kv.name));
    }

    private static void resetBuffer(FastKV kv, MappedByteBuffer buffer) throws IOException {
        if (buffer.capacity() != FastKV.PAGE_SIZE) {
            FileChannel channel = buffer == kv.aBuffer ? kv.aChannel : kv.bChannel;
            MappedByteBuffer newBuffer = truncateAndRemap(channel, FastKV.PAGE_SIZE);
            if (newBuffer == null) {
                throw new IOException("Failed to truncate and remap buffer");
            }
            if (buffer == kv.aBuffer) {
                kv.aBuffer = newBuffer;
            } else {
                kv.bBuffer = newBuffer;
            }
            buffer = newBuffer;
        }
        buffer.putInt(0, BufferHelper.packSize(0, kv.cipher != null));
        buffer.putLong(4, 0L);
    }

    static MappedByteBuffer truncateAndRemap(FileChannel channel, int newCapacity) {
        try {
            channel.truncate(newCapacity);
            return remapBuffer(channel, newCapacity);
        } catch (IOException e) {
            return null;
        }
    }

    static void resetMemory(FastKV kv) {
        kv.dataEnd = FastKV.DATA_START;
        kv.checksum = 0L;
        kv.data.clear();
        GCHelper.clearInvalid(kv);
        resetFastBuffer(kv);
    }

    private static void resetFastBuffer(FastKV kv) {
        if (kv.fastBuffer == null || kv.fastBuffer.hb.length != FastKV.PAGE_SIZE) {
            kv.fastBuffer = new FastBuffer(FastKV.PAGE_SIZE);
        } else {
            kv.fastBuffer.putLong(4, 0L);
        }
        kv.fastBuffer.putInt(0, BufferHelper.packSize(0, kv.cipher != null));
    }
}