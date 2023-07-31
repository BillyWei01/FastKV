package io.fastkv.fastkvdemo.fastkv.cipher;

import androidx.annotation.NonNull;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import io.fastkv.fastkvdemo.fastkv.FastKVLogger;
import io.fastkv.interfaces.FastCipher;

public class AESCipher implements FastCipher {
    private final SecureRandom random = new SecureRandom();
    private final byte[] iv = new byte[16];
    private final SecretKey secretKey;
    private final Cipher aesCipher;
    private final int ivMask;

    final NumberCipher numberCipher;

    public AESCipher(byte[] aesKey) {
        if (aesKey == null || aesKey.length != 16) {
            throw new IllegalArgumentException("Require a key with length of 16");
        }
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
        ByteBuffer ivBuffer = ByteBuffer.wrap(iv);
        ivBuffer.position(4);
        ivBuffer.putInt(r2.nextInt());
        ivBuffer.putInt(r1.nextInt());
        ivBuffer.putInt(r2.nextInt());

        secretKey = new SecretKeySpec(aesKey, "AES");
        aesCipher = getAesCipher();
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

    /**
     * Test AES encrypt/decrypt, if the function ok, return the cipher.
     */
    private Cipher getAesCipher() {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS7Padding");
            byte[] x = new byte[]{1, 2, 3, 4};
            byte[] y = aesEncrypt(cipher, x);
            byte[] z = aesDecrypt(cipher, y);
            if (Arrays.equals(x, z)) {
                return cipher;
            }
        } catch (Exception e) {
            logError(e);
        }
        return null;
    }

    @Override
    public byte[] encrypt(@NonNull byte[] src) {
        return aesCipher == null ? src : aesEncrypt(aesCipher, src);
    }

    @Override
    public byte[] decrypt(@NonNull byte[] dst) {
        return aesCipher == null ? dst : aesDecrypt(aesCipher, dst);
    }

    private synchronized byte[] aesEncrypt(Cipher aesCipher, @NonNull byte[] src) {
        try {
            // Because number of key-value is no much,
            // it's safety enough to use 32 bits random iv.
            // On the other hand, long iv needs more space.
            int s = random.nextInt();
            int v = s ^ ivMask;
            iv[0] = (byte) (v);
            iv[1] = (byte) (v >> 8);
            iv[2] = (byte) (v >> 16);
            iv[3] = (byte) (v >> 24);
            IvParameterSpec ivSpec = new IvParameterSpec(iv);
            aesCipher.init(Cipher.ENCRYPT_MODE, secretKey, ivSpec);
            byte[] cipherText = aesCipher.doFinal(src);
            byte[] result = new byte[4 + cipherText.length];
            result[0] = (byte) (s);
            result[1] = (byte) (s >> 8);
            result[2] = (byte) (s >> 16);
            result[3] = (byte) (s >> 24);
            System.arraycopy(cipherText, 0, result, 4, cipherText.length);
            return result;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private synchronized byte[] aesDecrypt(Cipher aesCipher, byte[] dst) {
        try {
            iv[0] = (byte) (dst[0] ^ ivMask);
            iv[1] = (byte) (dst[1] ^ (ivMask >> 8));
            iv[2] = (byte) (dst[2] ^ (ivMask >> 16));
            iv[3] = (byte) (dst[3] ^ (ivMask >> 24));
            IvParameterSpec ivSpec = new IvParameterSpec(iv);
            aesCipher.init(Cipher.DECRYPT_MODE, secretKey, ivSpec);
            return aesCipher.doFinal(dst, 4, dst.length - 4);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
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

    private void logError(Exception e) {
        FastKVLogger.INSTANCE.e("Cipher", e);
    }
}
