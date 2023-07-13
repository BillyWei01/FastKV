package io.fastkv.fastkvdemo.fastkv;

import androidx.annotation.NonNull;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import io.fastkv.interfaces.FastCipher;

public class AESCipher implements FastCipher {
    private final SecureRandom random = new SecureRandom();
    private final byte[] iv = new byte[16];
    private final SecretKey secretKey;
    private final Cipher aesCipher;
    private final int ivMask;

    // Option 1, use encryption operator of AES.
    private final NumberCipher numberCipher;

    // Option 2, use simple linear operation (like xor and cyclic shift).
//    private final int shift;
//    private final int mask32;
//    private final long mask64;

    public AESCipher(byte[] keyBytes) {
        if (keyBytes == null || keyBytes.length != 16) {
            throw new IllegalArgumentException("Require a key with length of 16");
        }
        secretKey = new SecretKeySpec(keyBytes, "AES");
        aesCipher = getAesCipher();

        // Use 'Random' with seed to make key expansion.
        long seed = ByteBuffer.wrap(keyBytes).getLong();
        Random r = new Random(seed);

        ivMask = r.nextInt();

        byte[] intKey = new byte[24];
        r.nextBytes(intKey);
        numberCipher = new NumberCipher(intKey);

//        shift = r.nextInt(23) + 10;
//        mask32 = r.nextInt();
//        mask64 = r.nextLong();
    }

    /**
     * Test AES encrypt/decrypt, if the function ok, return the cipher.
     */
    private Cipher getAesCipher() {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            byte[] x = new byte[]{1, 2, 3, 4};
            byte[] y = aesEncrypt(cipher, x);
            byte[] z = aesDecrypt(cipher, y);
            if(Arrays.equals(x, z)){
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
        // return ~Integer.rotateLeft(src ^ mask32, shift);
        return numberCipher.encryptInt(src);
    }

    @Override
    public int decrypt(int dst) {
        // return Integer.rotateRight(~dst, shift) ^ mask32;
        return numberCipher.decryptInt(dst);
    }

    @Override
    public long encrypt(long src) {
        // return ~Long.rotateLeft(src ^ mask64, shift);
        return numberCipher.encryptLong(src);
    }

    @Override
    public long decrypt(long dst) {
        // return Long.rotateRight(~dst, shift) ^ mask64;
        return numberCipher.decryptLong(dst);
    }

    private void logError(Exception e) {
        FastKVLogger.INSTANCE.e("Cipher", e);
    }
}
