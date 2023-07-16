package io.fastkv.fastkvdemo.fastkv;

/**
 * Use encryption operator of AES to encrypt/decrypt number.
 * <br>
 * Link to <a href="https://github.com/BillyWei01/LongEncrypt/tree/master">LongEncrypt</a>
 * and <a href="https://github.com/openluopworld/aes_128/blob/master/aes.c">aes.c</a>
 */
public class NumberCipher {
    private static final int ROUND = 2;
    private static final int KEY_LEN = (ROUND + 1) * 8; // 24

    private static final byte[] S_BOX = {
            99, 124, 119, 123, -14, 107, 111, -59, 48, 1, 103, 43, -2, -41, -85, 118,
            -54, -126, -55, 125, -6, 89, 71, -16, -83, -44, -94, -81, -100, -92, 114, -64,
            -73, -3, -109, 38, 54, 63, -9, -52, 52, -91, -27, -15, 113, -40, 49, 21,
            4, -57, 35, -61, 24, -106, 5, -102, 7, 18, -128, -30, -21, 39, -78, 117,
            9, -125, 44, 26, 27, 110, 90, -96, 82, 59, -42, -77, 41, -29, 47, -124,
            83, -47, 0, -19, 32, -4, -79, 91, 106, -53, -66, 57, 74, 76, 88, -49,
            -48, -17, -86, -5, 67, 77, 51, -123, 69, -7, 2, 127, 80, 60, -97, -88,
            81, -93, 64, -113, -110, -99, 56, -11, -68, -74, -38, 33, 16, -1, -13, -46,
            -51, 12, 19, -20, 95, -105, 68, 23, -60, -89, 126, 61, 100, 93, 25, 115,
            96, -127, 79, -36, 34, 42, -112, -120, 70, -18, -72, 20, -34, 94, 11, -37,
            -32, 50, 58, 10, 73, 6, 36, 92, -62, -45, -84, 98, -111, -107, -28, 121,
            -25, -56, 55, 109, -115, -43, 78, -87, 108, 86, -12, -22, 101, 122, -82, 8,
            -70, 120, 37, 46, 28, -90, -76, -58, -24, -35, 116, 31, 75, -67, -117, -118,
            112, 62, -75, 102, 72, 3, -10, 14, 97, 53, 87, -71, -122, -63, 29, -98,
            -31, -8, -104, 17, 105, -39, -114, -108, -101, 30, -121, -23, -50, 85, 40, -33,
            -116, -95, -119, 13, -65, -26, 66, 104, 65, -103, 45, 15, -80, 84, -69, 22
    };

    private static final byte[] INV_S_BOX = {
            82, 9, 106, -43, 48, 54, -91, 56, -65, 64, -93, -98, -127, -13, -41, -5,
            124, -29, 57, -126, -101, 47, -1, -121, 52, -114, 67, 68, -60, -34, -23, -53,
            84, 123, -108, 50, -90, -62, 35, 61, -18, 76, -107, 11, 66, -6, -61, 78,
            8, 46, -95, 102, 40, -39, 36, -78, 118, 91, -94, 73, 109, -117, -47, 37,
            114, -8, -10, 100, -122, 104, -104, 22, -44, -92, 92, -52, 93, 101, -74, -110,
            108, 112, 72, 80, -3, -19, -71, -38, 94, 21, 70, 87, -89, -115, -99, -124,
            -112, -40, -85, 0, -116, -68, -45, 10, -9, -28, 88, 5, -72, -77, 69, 6,
            -48, 44, 30, -113, -54, 63, 15, 2, -63, -81, -67, 3, 1, 19, -118, 107,
            58, -111, 17, 65, 79, 103, -36, -22, -105, -14, -49, -50, -16, -76, -26, 115,
            -106, -84, 116, 34, -25, -83, 53, -123, -30, -7, 55, -24, 28, 117, -33, 110,
            71, -15, 26, 113, 29, 41, -59, -119, 111, -73, 98, 14, -86, 24, -66, 27,
            -4, 86, 62, 75, -58, -46, 121, 32, -102, -37, -64, -2, 120, -51, 90, -12,
            31, -35, -88, 51, -120, 7, -57, 49, -79, 18, 16, 89, 39, -128, -20, 95,
            96, 81, 127, -87, 25, -75, 74, 13, 45, -27, 122, -97, -109, -55, -100, -17,
            -96, -32, 59, 77, -82, 42, -11, -80, -56, -21, -69, 60, -125, 83, -103, 97,
            23, 43, 4, 126, -70, 119, -42, 38, -31, 105, 20, 99, 85, 33, 12, 125
    };

    /*
    private static byte mul2(byte a) {
        return (byte) (((a & 0x80) != 0) ? ((a << 1) ^ 0x1b) : (a << 1));
    }
    */
    private static final byte[] mul2 = new byte[256];
    static {
        for (int i = 0; i < 128; i++) {
            mul2[i] = (byte) (i << 1);
        }
        for (int i = 128; i < 256; i++) {
            mul2[i] = (byte) ((i << 1) ^ 0x1b);
        }
    }

    private final byte[] key;

    /**
     * @param key require a key with length of 24.
     */
    public NumberCipher(byte[] key) {
        if (key == null || key.length != KEY_LEN) {
            throw new IllegalArgumentException("The key must be length of " + KEY_LEN);
        }
        this.key = key;
    }

    public long encryptLong(long value) {
        byte[] state = long2Bytes(value);
        for (int i = 0; i < ROUND; i++) {
            int offset = i << 3;
            for (int j = 0; j < 8; j++) {
                // AddRoundKey and SubBytes
                state[j] = S_BOX[(state[j] ^ key[offset + j]) & 0xFF];
            }
            shift_rows(state);
            multiply(state);
            multiply_4(state);
        }
        for (int j = 0; j < 8; j++) {
            state[j] ^= key[(ROUND << 3) + j];
        }
        return bytes2Long(state);
    }

    public long decryptLong(long value) {
        byte[] state = long2Bytes(value);
        for (int j = 0; j < 8; j++) {
            state[j] ^= key[(ROUND << 3) + j];
        }
        for (int i = ROUND - 1; i >= 0; i--) {
            inv_multiply(state, 0);
            inv_multiply(state, 4);
            inv_shift_rows(state);
            int offset = i << 3;
            for (int j = 0; j < 8; j++) {
                state[j] = (byte) (INV_S_BOX[state[j] & 0xFF] ^ key[offset + j]);
            }
        }
        return bytes2Long(state);
    }

    public int encryptInt(int value) {
        byte[] state = int2Bytes(value);
        for (int i = 0; i < ROUND; i++) {
            int offset = i << 2;
            for (int j = 0; j < 4; j++) {
                state[j] = S_BOX[(state[j] ^ key[offset + j]) & 0xFF];
            }
            multiply(state);
        }
        for (int j = 0; j < 4; j++) {
            state[j] ^= key[(ROUND << 2) + j];
        }
        return bytes2Int(state);
    }

    public int decryptInt(int value) {
        byte[] state = long2Bytes(value);
        for (int j = 0; j < 4; j++) {
            state[j] ^= key[(ROUND << 2) + j];
        }
        for (int i = ROUND - 1; i >= 0; i--) {
            inv_multiply(state, 0);
            for (int j = 0; j < 4; j++) {
                int offset = i << 2;
                state[j] = (byte) (INV_S_BOX[state[j] & 0xFF] ^ key[offset + j]);
            }
        }
        return bytes2Int(state);
    }

    /*
     * [b0]	  [02 03 01 01]   [b0]
     * [b1]	= [01 02 03 01] . [b1]
     * [b2]	  [01 01 02 03]   [b2]
     * [b3]	  [03 01 01 02]   [b3]
     */
    private static void multiply(byte[] b) {
        byte a0 = (byte) (b[0] ^ b[1]);
        byte a1 = (byte) (b[1] ^ b[2]);
        byte a2 = (byte) (b[2] ^ b[3]);
        byte a3 = (byte) (b[3] ^ b[0]);
        byte t = (byte) (a0 ^ a2);
        b[0] ^= mul2[a0 & 0xFF] ^ t;
        b[1] ^= mul2[a1 & 0xFF] ^ t;
        b[2] ^= mul2[a2 & 0xFF] ^ t;
        b[3] ^= mul2[a3 & 0xFF] ^ t;
    }

    private static void multiply_4(byte[] b) {
        byte a0 = (byte) (b[4] ^ b[5]);
        byte a1 = (byte) (b[5] ^ b[6]);
        byte a2 = (byte) (b[6] ^ b[7]);
        byte a3 = (byte) (b[7] ^ b[4]);
        byte t = (byte) (a0 ^ a2);
        b[4] ^= mul2[a0 & 0xFF] ^ t;
        b[5] ^= mul2[a1 & 0xFF] ^ t;
        b[6] ^= mul2[a2 & 0xFF] ^ t;
        b[7] ^= mul2[a3 & 0xFF] ^ t;
    }

    /*
     * [d0]	  [0e 0b 0d 09]   [b0]
     * [d1]	= [09 0e 0b 0d] . [b1]
     * [d2]	  [0d 09 0e 0b]   [b2]
     * [d3]	  [0b 0d 09 0e]   [b3]
     */
    private static void inv_multiply(byte[] b, int i) {
        byte u = (byte) (b[i] ^ b[i + 2]);
        byte v = (byte) (b[i + 1] ^ b[i + 3]);
        if (i == 0) {
            multiply(b);
        } else if (i == 4) {
            multiply_4(b);
        } else {
            throw new IllegalArgumentException("invalid i:" + i);
        }
        u = mul2[mul2[u & 0xFF] & 0xFF];
        v = mul2[mul2[v & 0xFF] & 0xFF];
        byte t = mul2[(u ^ v) & 0xFF];
        u ^= t;
        v ^= t;
        b[i] ^= u;
        b[i + 1] ^= v;
        b[i + 2] ^= u;
        b[i + 3] ^= v;
    }

    private static void shift_rows(byte[] state) {
        byte t1 = state[7];
        byte t0 = state[6];
        state[7] = state[5];
        state[6] = state[4];
        state[5] = state[3];
        state[4] = state[2];
        state[3] = state[1];
        state[2] = state[0];
        state[1] = t1;
        state[0] = t0;
    }

    private static void inv_shift_rows(byte[] state) {
        byte t0 = state[0];
        byte t1 = state[1];
        state[0] = state[2];
        state[1] = state[3];
        state[2] = state[4];
        state[3] = state[5];
        state[4] = state[6];
        state[5] = state[7];
        state[6] = t0;
        state[7] = t1;
    }

    public static byte[] int2Bytes(int value) {
        byte[] state = new byte[4];
        state[3] = (byte) (value >> 24);
        state[2] = (byte) (value >> 16);
        state[1] = (byte) (value >> 8);
        state[0] = (byte) value;
        return state;
    }

    public static int bytes2Int(byte[] state) {
        return (((state[3] & 0xFF) << 24) +
                ((state[2] & 0xFF) << 16) +
                ((state[1] & 0xFF) << 8) +
                (state[0] & 0xFF));
    }

    public static byte[] long2Bytes(long value) {
        byte[] state = new byte[8];
        state[7] = (byte) (value >> 56);
        state[6] = (byte) (value >> 48);
        state[5] = (byte) (value >> 40);
        state[4] = (byte) (value >> 32);
        state[3] = (byte) (value >> 24);
        state[2] = (byte) (value >> 16);
        state[1] = (byte) (value >> 8);
        state[0] = (byte) value;
        return state;
    }

    public static long bytes2Long(byte[] state) {
        return (((long) state[7]) << 56) +
                ((long) (state[6] & 0xFF) << 48) +
                ((long) (state[5] & 0xFF) << 40) +
                ((long) (state[4] & 0xFF) << 32) +
                ((long) (state[3] & 0xFF) << 24) +
                ((long) (state[2] & 0xFF) << 16) +
                ((long) (state[1] & 0xFF) << 8) +
                ((long) (state[0] & 0xFF));
    }
}

