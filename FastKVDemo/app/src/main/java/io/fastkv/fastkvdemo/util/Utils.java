package io.fastkv.fastkvdemo.util;


import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

public class Utils {
    private static final char[] HEX_DIGITS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f'};

    public static byte[] md5(byte[] msg) throws NoSuchAlgorithmException {
        return MessageDigest.getInstance("MD5").digest(msg);
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

    @SuppressWarnings({"rawtypes", "unchecked", "ConstantConditions"})
    public static int getPageSize() {
        try {
            Class unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Method method = unsafeClass.getDeclaredMethod("pageSize");
            method.setAccessible(true);
            return (int) (Integer) method.invoke(theUnsafe.get(null));
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return 0;
    }


    /**
     * Get array with normal distribution.
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
