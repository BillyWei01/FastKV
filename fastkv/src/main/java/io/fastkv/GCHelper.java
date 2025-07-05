package io.fastkv;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import io.fastkv.Container.BaseContainer;
import io.fastkv.Container.VarContainer;

/**
 * 垃圾回收辅助类，使用扩展方法模式处理FastKV的内存管理和垃圾回收。
 *
 * <h2>GC执行策略</h2>
 * 
 * <h3>触发条件（双重阈值机制）</h3>
 * FastKV使用双重阈值来决定何时触发垃圾回收：
 * <ul>
 * <li><b>字节阈值</b>：当无效数据达到16KB时触发（基础阈值8KB的2倍）</li>
 * <li><b>键数量阈值</b>：当无效键数量达到阈值时触发
 *     <ul>
 *     <li>小数据量（&lt;16KB）：80个无效键</li>
 *     <li>大数据量（≥16KB）：160个无效键</li>
 *     </ul>
 * </li>
 * </ul>
 * 满足任一条件即触发GC，确保在数据碎片化严重时及时清理。
 * 
 * <h3>GC执行流程</h3>
 * <ol>
 * <li><b>合并无效段</b>：将相邻的无效内存段合并，减少后续处理复杂度</li>
 * <li><b>数据压缩</b>：将有效数据向前移动，填补无效数据留下的空隙</li>
 * <li><b>偏移量更新</b>：更新所有Container对象的内存偏移量</li>
 * <li><b>校验和重算</b>：根据压缩范围选择增量或全量重算校验和</li>
 * <li><b>缓冲区同步</b>：将压缩后的数据同步到A/B文件（非阻塞模式）</li>
 * <li><b>缓冲区截断</b>：如果空闲空间过大，截断缓冲区以节省内存</li>
 * </ol>
 * 
 * <h3>内存管理策略</h3>
 * <ul>
 * <li><b>预防式GC</b>：在空间不足时，优先尝试GC而非直接扩容</li>
 * <li><b>智能扩容</b>：只有在GC后仍空间不足时才扩容缓冲区</li>
 * <li><b>自适应截断</b>：GC后如果空闲空间超过32KB则截断缓冲区</li>
 * <li><b>分级阈值</b>：根据数据量大小动态调整键数量阈值</li>
 * </ul>
 * 
 * <h3>性能优化</h3>
 * <ul>
 * <li><b>段合并优化</b>：减少内存拷贝次数和处理复杂度</li>
 * <li><b>增量校验和</b>：避免不必要的全量校验和计算</li>
 * <li><b>批量偏移更新</b>：使用数组记录偏移映射，批量更新</li>
 * <li><b>原地压缩</b>：在同一缓冲区内完成数据移动，避免额外内存分配</li>
 * </ul>
 */
class GCHelper {
    static final String TRUNCATE_FINISH = "truncate finish";
    static final String GC_FINISH = "gc finish";

    private static final int GC_KEYS_THRESHOLD = 100;

    private static final int GC_BYTES_THRESHOLD = 8192;

    private static final int TRUNCATE_THRESHOLD = 32768; // 32KB

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
     * 执行垃圾回收 
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

        LoggerHelper.info(kv, GC_FINISH);
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
        if (kv.fastBuffer.hb.length - expectedEnd > TRUNCATE_THRESHOLD) {
            truncate(kv, expectedEnd);
        }
    }

    static int getTruncateThreshold() {
        return Math.max(FastKV.PAGE_SIZE, 1 << 15);
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
        LoggerHelper.info(kv, TRUNCATE_FINISH);
    }

    /**
     * 确保缓冲区大小 
     * 
     * @param kv FastKV实例
     * @param allocate 需要分配的空间大小
     */
    static void ensureSize(FastKV kv, int allocate) {
        int capacity = kv.fastBuffer.hb.length;
        int expected = kv.dataEnd + allocate;
        if (expected >= capacity) {
            if (kv.invalidBytes > allocate && kv.invalidBytes > GC_BYTES_THRESHOLD) {
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
     * 检查是否需要垃圾回收 
     * 
     * @param kv FastKV实例
     */
    static void checkGC(FastKV kv) {
        if (kv.invalidBytes >= GC_BYTES_THRESHOLD  || kv.invalids.size() >= GC_KEYS_THRESHOLD) {
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