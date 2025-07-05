package io.fastkv;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import io.fastkv.Container.BaseContainer;
import io.fastkv.Container.VarContainer;

/**
 * 垃圾回收辅助类，使用扩展方法模式处理FastKV的内存管理和垃圾回收。
 * 类似Kotlin的扩展方法，第一个参数总是FastKV实例。
 */
class GCHelper {
    
    /**
     * 合并无效段以加速GC。
     * 将相邻的内存段合并成更大的段，减少GC时的处理复杂度。
     * 
     * @param invalids 无效段列表，会被就地修改
     */
    static void mergeInvalids(ArrayList<Segment> invalids) {
        if (invalids.isEmpty()) {
            return;
        }
        
        Collections.sort(invalids);
        int index = 0;
        Segment p = invalids.get(0);
        int n = invalids.size();
        
        for (int i = 1; i < n; i++) {
            Segment q = invalids.get(i);
            if (q.start == p.end) {
                // 相邻段，合并
                p.end = q.end;
            } else {
                // 不相邻，保留当前段
                index++;
                if (index != i) {
                    invalids.set(index, q);
                }
                p = q;
            }
        }
        
        // 清理多余的段
        index++;
        if (n > index) {
            invalids.subList(index, n).clear();
        }
    }
    
    /**
     * 执行垃圾回收 - 扩展方法模式
     * 
     * @param kv FastKV实例
     * @param allocate 需要分配的空间大小
     */
    static void gc(FastKV kv, int allocate) {
        mergeInvalids(kv.invalids);

        final Segment head = kv.invalids.get(0);
        final int gcStart = head.start;
        final int newDataEnd = kv.dataEnd - kv.invalidBytes;
        final int newDataSize = newDataEnd - FastKV.DATA_START;
        final int gcUpdateSize = newDataEnd - gcStart;
        final int gcSize = kv.dataEnd - gcStart;
        final boolean fullChecksum = newDataSize < gcSize + gcUpdateSize;
        if (!fullChecksum) {
            kv.checksum ^= kv.fastBuffer.getChecksum(gcStart, gcSize);
        }
        // 压缩并记录偏移
        int n = kv.invalids.size();
        final int remain = kv.dataEnd - kv.invalids.get(n - 1).end;
        int shiftCount = (remain > 0) ? n : n - 1;
        int[] src = new int[shiftCount];
        int[] shift = new int[shiftCount];
        int desPos = head.start;
        int srcPos = head.end;
        for (int i = 1; i < n; i++) {
            Segment q = kv.invalids.get(i);
            int size = q.start - srcPos;
            System.arraycopy(kv.fastBuffer.hb, srcPos, kv.fastBuffer.hb, desPos, size);
            int index = i - 1;
            src[index] = srcPos;
            shift[index] = srcPos - desPos;
            desPos += size;
            srcPos = q.end;
        }
        if (remain > 0) {
            System.arraycopy(kv.fastBuffer.hb, srcPos, kv.fastBuffer.hb, desPos, remain);
            int index = n - 1;
            src[index] = srcPos;
            shift[index] = srcPos - desPos;
        }
        clearInvalid(kv);

        if (fullChecksum) {
            kv.checksum = kv.fastBuffer.getChecksum(FastKV.DATA_START, newDataEnd - FastKV.DATA_START);
        } else {
            kv.checksum ^= kv.fastBuffer.getChecksum(gcStart, newDataEnd - gcStart);
        }
        kv.dataEnd = newDataEnd;

        updateBuffer(kv, gcStart, allocate, gcUpdateSize);

        updateOffset(kv, gcStart, src, shift);

        LoggerHelper.info(kv, FastKV.GC_FINISH);
    }

    /**
     * 更新容器偏移量
     */
    private static void updateOffset(FastKV kv, int gcStart, int[] srcArray, int[] shiftArray) {
        Collection<BaseContainer> values = kv.data.values();
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

    /**
     * 更新缓冲区
     */
    private static void updateBuffer(FastKV kv, int gcStart, int allocate, int gcUpdateSize) {
        int newDataSize = kv.dataEnd - FastKV.DATA_START;
        int packedSize = BufferHelper.packSize(newDataSize, kv.cipher != null);
        if (kv.writingMode == FastKV.NON_BLOCKING) {
            kv.aBuffer.putInt(0, -1);
            kv.aBuffer.putLong(4, kv.checksum);
            kv.aBuffer.position(gcStart);
            kv.aBuffer.put(kv.fastBuffer.hb, gcStart, gcUpdateSize);
            kv.aBuffer.putInt(0, packedSize);
            kv.bBuffer.putInt(0, packedSize);
            kv.bBuffer.putLong(4, kv.checksum);
            kv.bBuffer.position(gcStart);
            kv.bBuffer.put(kv.fastBuffer.hb, gcStart, gcUpdateSize);
        } else {
            kv.fastBuffer.putInt(0, packedSize);
            kv.fastBuffer.putLong(4, kv.checksum);
        }

        int expectedEnd = kv.dataEnd + allocate;
        if (kv.fastBuffer.hb.length - expectedEnd > BufferHelper.getTruncateThreshold()) {
            truncate(kv, expectedEnd);
        }
    }

    /**
     * 截断缓冲区
     */
    private static void truncate(FastKV kv, int expectedEnd) {
        // 至少保留一页空间
        int newCapacity = FileHelper.getNewCapacity(FastKV.PAGE_SIZE, expectedEnd + FastKV.PAGE_SIZE);
        if (newCapacity >= kv.fastBuffer.hb.length) {
            return;
        }
        byte[] bytes = new byte[newCapacity];
        System.arraycopy(kv.fastBuffer.hb, 0, bytes, 0, kv.dataEnd);
        kv.fastBuffer.hb = bytes;
        if (kv.writingMode == FastKV.NON_BLOCKING) {
            MappedByteBuffer newABuffer = FileHelper.truncateAndRemap(kv.aChannel, newCapacity);
            MappedByteBuffer newBBuffer = FileHelper.truncateAndRemap(kv.bChannel, newCapacity);
            if (newABuffer == null || newBBuffer == null) {
                LoggerHelper.error(kv, new Exception(FileHelper.MAP_FAILED));
                FileHelper.toBlockingMode(kv);
            } else {
                kv.aBuffer = newABuffer;
                kv.bBuffer = newBBuffer;
            }
        }
        LoggerHelper.info(kv, FastKV.TRUNCATE_FINISH);
    }

    /**
     * 确保缓冲区大小 - 扩展方法模式
     * 
     * @param kv FastKV实例
     * @param allocate 需要分配的空间大小
     */
    static void ensureSize(FastKV kv, int allocate) {
        int capacity = kv.fastBuffer.hb.length;
        int expected = kv.dataEnd + allocate;
        if (expected >= capacity) {
            if (kv.invalidBytes > allocate && kv.invalidBytes > BufferHelper.calculateBytesThreshold(kv.dataEnd, 8192)) {
                gc(kv, allocate);
            } else {
                int newCapacity = FileHelper.getNewCapacity(capacity, expected);
                byte[] bytes = new byte[newCapacity];
                System.arraycopy(kv.fastBuffer.hb, 0, bytes, 0, kv.dataEnd);
                kv.fastBuffer.hb = bytes;
                if (kv.writingMode == FastKV.NON_BLOCKING) {
                    MappedByteBuffer newABuffer = FileHelper.remapBuffer(kv.aChannel, newCapacity);
                    MappedByteBuffer newBBuffer = FileHelper.remapBuffer(kv.bChannel, newCapacity);
                    if (newABuffer == null || newBBuffer == null) {
                        LoggerHelper.error(kv, new Exception(FileHelper.MAP_FAILED));
                        int packedSize = BufferHelper.packSize(kv.dataEnd - FastKV.DATA_START, kv.cipher != null);
                        kv.fastBuffer.putInt(0, packedSize);
                        kv.fastBuffer.putLong(4, kv.checksum);
                        FileHelper.toBlockingMode(kv);
                    } else {
                        kv.aBuffer = newABuffer;
                        kv.bBuffer = newBBuffer;
                    }
                }
            }
        }
    }

    /**
     * 检查是否需要垃圾回收 - 扩展方法模式
     * 
     * @param kv FastKV实例
     */
    static void checkGC(FastKV kv) {
        int bytesThreshold = BufferHelper.calculateBytesThreshold(kv.dataEnd, 8192);
        int keysThreshold = kv.dataEnd < (1 << 14) ? 80 : 80 << 1;
        if (kv.invalidBytes >= (bytesThreshold << 1) || kv.invalids.size() >= keysThreshold) {
            gc(kv, 0);
        }
    }

    static void clearInvalid(FastKV kv) {
        kv.invalidBytes = 0;
        kv.invalids.clear();
    }

    static void countInvalid(FastKV kv, int start, int end) {
        kv.invalidBytes += (end - start);
        kv.invalids.add(new Segment(start, end));
    }
}