package io.fastkv;

/**
 * 缓冲区辅助类，提供数据处理相关的工具方法。
 */
class BufferHelper {
    private static final int CIPHER_MASK = 1 << 30;

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
     * 打包数据大小（添加加密标记）
     *
     * @param size      原始大小
     * @param hasCipher 是否加密
     * @return 打包后的大小
     */
    static int packSize(int size, boolean hasCipher) {
        return hasCipher ? size | CIPHER_MASK : size;
    }
    
    /**
     * 解包数据大小（移除加密标记）
     *
     * @param size 打包的大小
     * @return 原始大小
     */
    static int unpackSize(int size) {
        return size & (~CIPHER_MASK);
    }
    
    /**
     * 检查是否加密
     *
     * @param size 打包的大小
     * @return 是否加密
     */
    static boolean isCipher(int size) {
        return (size & CIPHER_MASK) != 0;
    }
} 