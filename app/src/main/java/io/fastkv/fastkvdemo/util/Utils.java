package io.fastkv.fastkvdemo.util;


import android.annotation.SuppressLint;
import android.util.Log;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

public class Utils {
    private static final char[] HEX_DIGITS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f'};

    private static final int PAGE_SIZE_16K = 16 * 1024;
    private static int sPageSize = 0;

    public static byte[] getMD5Array(byte[] msg) throws NoSuchAlgorithmException {
        return MessageDigest.getInstance("MD5").digest(msg);
    }

    @SuppressWarnings({"rawtypes", "unchecked", "ConstantConditions"})
    @SuppressLint("DiscouragedPrivateApi")
    private static int getPageSize() {
        try {
            Class unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Method method = unsafeClass.getDeclaredMethod("pageSize");
            method.setAccessible(true);
            int pageSize = (int) (Integer) method.invoke(theUnsafe.get(null));
            Log.d("MyTag", "page size: " + pageSize);
            return pageSize;
        } catch (Throwable ignore) {
        }
        Log.d("MyTag", "DEFAULT_PAGE_SIZE: " + PAGE_SIZE_16K);
        return PAGE_SIZE_16K;
    }

    /**
     * 系统page size是否是 16K。
     * Android新版本会逐步版本会开启16K page size, 未适配的so会崩溃。
     * 目前 MMKV 还没适配16K, 所以判断系统 page size 是16K则不自行MMKV的性能测试。
     */
    public static boolean is16KPageSize() {
        if (sPageSize == 0) {
            sPageSize = getPageSize() ;
        }
        return sPageSize == PAGE_SIZE_16K;
    }

    public static String getMD5(byte[] msg) {
        if (msg == null) {
            return "";
        }
        try {
            return bytes2Hex(getMD5Array(msg));
        } catch (Exception ignore) {
        }
        return bytes2Hex(Arrays.copyOf(msg, 16));
    }

    public static String bytes2Hex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        int len = bytes.length;
        char[] buf = new char[len << 1];
        for (int i = 0; i < len; i++) {
            int b = bytes[i];
            int index = i << 1;
            buf[index] = HEX_DIGITS[(b >> 4) & 0xF];
            buf[index + 1] = HEX_DIGITS[b & 0xF];
        }
        return new String(buf);
    }

    public static String formatTime(long t) {
        Date date = new Date(t);
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.getDefault());
        return sd.format(date);
    }

    /**
     * 获取正态分布的数据
     */
    public static int[] getDistributedArray(int n, int times, Random r) {
        int avg = n / 2;
        int v;
        if (n <= 50) {
            v = n;
        } else if (n <= 100) {
            v = (int) (n * 1.5);
        } else if (n <= 200) {
            v = n * 2;
        } else {
            v = n * 3;
        }
        int count = n * times;
        int[] a = new int[count];
        for (int i = 0; i < count; ) {
            int x = (int) (Math.sqrt(v) * r.nextGaussian() + avg);
            if (x >= 0 && x < n) {
                a[i++] = x;
            }
        }
        return a;
    }
}

