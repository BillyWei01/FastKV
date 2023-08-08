package io.fastkv.fastkvdemo.fastkv.cipher;

import androidx.annotation.NonNull;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.BadPaddingException;

import io.fastkv.interfaces.FastCipher;
import io.github.fastaes.FastAES;

public class AESCipher implements FastCipher {
    private static final int MAX_IV_COUNT = 4;
    private static final byte[][] sIvPool = new byte[MAX_IV_COUNT][];
    private static int sCount = 0;

    private final SecureRandom random = new SecureRandom();
    private final byte[] aesKey;
    private final int ivMask;

    final NumberCipher numberCipher;

    public AESCipher(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("key can't be null");
        }
        byte[] digest = md5(key);
        byte[] numberKey = new byte[NumberCipher.KEY_LEN];
        System.arraycopy(digest, 0, numberKey, 0, 16);
        if (key.length > 16) {
            int len = Math.min(NumberCipher.KEY_LEN - 16, key.length - 16);
            System.arraycopy(key, 16, numberKey, 16, len);
        }
        numberCipher = new NumberCipher(numberKey);
        ivMask = ByteBuffer.wrap(numberKey, 16, 4).getInt();
        aesKey = Arrays.copyOf(key, 16);
    }

    private byte[] md5(byte[] bytes) {
        try {
            return MessageDigest.getInstance("MD5").digest(bytes);
        } catch (Exception ignore) {
        }
        return new byte[16];
    }

    private byte[] getIV() {
        synchronized (sIvPool) {
            if (sCount > 0) {
                byte[] iv = sIvPool[--sCount];
                sIvPool[sCount] = null;
                return iv;
            } else {
                return new byte[16];
            }
        }
    }

    private void recycleIV(byte[] bytes) {
        synchronized (sIvPool) {
            if (sCount < MAX_IV_COUNT) {
                sIvPool[sCount++] = bytes;
            }
        }
    }

    @Override
    public byte[] encrypt(@NonNull byte[] src) {
        // AES CBC 的初始向量时16个字节，但我们只随机化其前4个字节（只记录4字节的随机数可节约空间）。
        // 然后就是，因为key-value的数量不是很多，32bits的随机初始向量足以提供随机性。
        int s = random.nextInt();
        int v = s ^ ivMask;
        byte[] iv = getIV();
        iv[0] = (byte) (v);
        iv[1] = (byte) (v >> 8);
        iv[2] = (byte) (v >> 16);
        iv[3] = (byte) (v >> 24);

        // 一般而言加密是不会失败的，除非内存不足了。
        // 加密过程可能需要动态申请内存，申请不到内存自然就无法加密了，这时候会抛异常。
        // 这里是否需要catch?
        // 不catch的话，可能APP崩溃；
        // catch的话，这里是不崩溃了，但是可能会导致数据状态不合预期。
        byte[] cipherText;
        try {
            cipherText = FastAES.encrypt(src, aesKey, iv);
        } catch (Exception e) {
            return null;
        }
        recycleIV(iv);
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
        byte[] iv = getIV();
        iv[0] = (byte) (dst[0] ^ ivMask);
        iv[1] = (byte) (dst[1] ^ (ivMask >> 8));
        iv[2] = (byte) (dst[2] ^ (ivMask >> 16));
        iv[3] = (byte) (dst[3] ^ (ivMask >> 24));
        try {
            byte[] cipherText = Arrays.copyOfRange(dst, 4, dst.length);
            byte[] result = FastAES.decrypt(cipherText, aesKey, iv);
            recycleIV(iv);
            return result;
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
