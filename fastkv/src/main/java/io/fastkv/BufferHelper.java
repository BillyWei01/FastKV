package io.fastkv;

/**
 * 缓冲区辅助类，提供数据处理相关的工具方法。
 */
class BufferHelper {
    
    /**
     * 计算校验和的位移
     * 
     * @param checkSum 原始校验和
     * @param offset 偏移量
     * @return 位移后的校验和
     */
    static long shiftCheckSum(long checkSum, int offset) {
        int shift = (offset & 7) << 3;
        return (checkSum << shift) | (checkSum >>> (64 - shift));
    }
    
    /**
     * 计算GC阈值
     * 
     * @param dataEnd 数据结束位置
     * @param baseThreshold 基础阈值
     * @return 计算后的阈值
     */
    static int calculateBytesThreshold(int dataEnd, int baseThreshold) {
        if (dataEnd <= (1 << 14)) {
            return baseThreshold;
        } else {
            return baseThreshold << 1;
        }
    }
    
    /**
     * 打包数据大小（添加加密标记）
     * 
     * @param size 原始大小
     * @param hasCipher 是否加密
     * @param cipherMask 加密掩码
     * @return 打包后的大小
     */
    static int packSize(int size, boolean hasCipher, int cipherMask) {
        return hasCipher ? size | cipherMask : size;
    }
    
    /**
     * 解包数据大小（移除加密标记）
     * 
     * @param size 打包的大小
     * @param cipherMask 加密掩码
     * @return 原始大小
     */
    static int unpackSize(int size, int cipherMask) {
        return size & (~cipherMask);
    }
    
    /**
     * 检查是否加密
     * 
     * @param size 打包的大小
     * @param cipherMask 加密掩码
     * @return 是否加密
     */
    static boolean isCipher(int size, int cipherMask) {
        return (size & cipherMask) != 0;
    }

    /**
     * 获取截断阈值
     * 
     * @return 截断阈值
     */
    static int getTruncateThreshold() {
        return Math.max(Utils.getPageSize(), 1 << 15);
    }
} 