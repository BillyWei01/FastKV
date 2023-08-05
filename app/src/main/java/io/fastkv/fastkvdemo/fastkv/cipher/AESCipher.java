package io.fastkv.fastkvdemo.fastkv.cipher;

import androidx.annotation.NonNull;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.BadPaddingException;

import io.fastkv.interfaces.FastCipher;
import io.github.fastaes.FastAES;

public class AESCipher implements FastCipher {
    private final SecureRandom random = new SecureRandom();
    private final byte[] aesKey;
    private final int ivMask;

    final NumberCipher numberCipher;

    public AESCipher(byte[] aesKey) {
        if (aesKey == null || aesKey.length != 16) {
            throw new IllegalArgumentException("Require a key with length of 16");
        }
        this.aesKey = aesKey;

        // Use 'pseudo-random' to make key expansion.
        long seed = ByteBuffer.wrap(aesKey).getLong();
        CustomRandom r1 = new CustomRandom(seed & 0xFFFFFFFFL);
        CustomRandom r2 = new CustomRandom(seed >>> 32);

        byte[] intKey = new byte[NumberCipher.KEY_LEN];
        ByteBuffer buffer = ByteBuffer.wrap(intKey);
        int n = NumberCipher.KEY_LEN / 8;
        for (int i = 0; i < n; i++) {
            buffer.putInt(r1.nextInt());
            buffer.putInt(r2.nextInt());
        }
        numberCipher = new NumberCipher(intKey);

        ivMask = r1.nextInt();
    }

    // Use custom random instead of 'Random' of JDK,
    // in case of different JDKs having different implementations.
    private static class CustomRandom {
        private static final long multiplier = 0x5DEECE66DL;
        private static final long addend = 0xBL;
        private static final long mask = (1L << 48) - 1;
        private long seed;

        CustomRandom(long seed) {
            this.seed = seed;
        }

        protected int nextInt() {
            long oldSeed = seed;
            do {
                seed = (oldSeed * multiplier + addend) & mask;
                if (oldSeed != seed) {
                    break;
                }
                oldSeed++;
            } while (true);
            return (int) (seed >>> 12);
        }
    }

    @Override
    public byte[] encrypt(@NonNull byte[] src) {
        // AES CBC 的初始向量时16个字节，但我们只随机化其前4个字节（只记录4字节的随机数可节约空间）。
        // 然后就是，因为key-value的数量不是很多，32bits的随机初始向量足以提供随机性。
        int s = random.nextInt();
        int v = s ^ ivMask;
        byte[] iv = new byte[16];
        iv[0] = (byte) (v);
        iv[1] = (byte) (v >> 8);
        iv[2] = (byte) (v >> 16);
        iv[3] = (byte) (v >> 24);
        byte[] cipherText = FastAES.encrypt(src, aesKey, iv);
        byte[] result = new byte[4 + cipherText.length];
        result[0] = (byte) (s);
        result[1] = (byte) (s >> 8);
        result[2] = (byte) (s >> 16);
        result[3] = (byte) (s >> 24);
        System.arraycopy(cipherText, 0, result, 4, cipherText.length);
        return result;
    }

    @Override
    public byte[] decrypt(@NonNull byte[] dst) {
        byte[] iv = new byte[16];
        iv[0] = (byte) (dst[0] ^ ivMask);
        iv[1] = (byte) (dst[1] ^ (ivMask >> 8));
        iv[2] = (byte) (dst[2] ^ (ivMask >> 16));
        iv[3] = (byte) (dst[3] ^ (ivMask >> 24));
        try {
            byte[] cipherText = Arrays.copyOfRange(dst, 4, dst.length);
            return FastAES.decrypt(cipherText, aesKey, iv);
        } catch (BadPaddingException e) {
            throw new IllegalStateException("Bad padding", e);
        }
    }

    @Override
    public int encrypt(int src) {
        return numberCipher.encryptInt(src);
    }

    @Override
    public int decrypt(int dst) {
        return numberCipher.decryptInt(dst);
    }

    @Override
    public long encrypt(long src) {
        return numberCipher.encryptLong(src);
    }

    @Override
    public long decrypt(long dst) {
        return numberCipher.decryptLong(dst);
    }
}
