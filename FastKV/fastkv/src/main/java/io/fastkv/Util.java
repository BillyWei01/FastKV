package io.fastkv;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.SecureRandom;

class Util {
    private static class Holder {
        static final SecureRandom random = new SecureRandom();
        static final char[] digits = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'a', 'b', 'c', 'd', 'e', 'f'};
    }

    static final int NAME_SIZE = 32;

    /**
     * Like UUID.randomUUID().toString(), but without '-'.
     */
    static String randomName() {
        int len = NAME_SIZE >> 1;
        byte[] bytes = new byte[len];
        Holder.random.nextBytes(bytes);
        char[] buf = new char[NAME_SIZE];
        for (int i = 0; i < len; i++) {
            int b = bytes[i];
            int index = i << 1;
            buf[index] = Holder.digits[(b >> 4) & 0xF];
            buf[index + 1] = Holder.digits[b & 0xF];
        }
        return new String(buf);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static int getPageSize() {
        try {
            Class unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Method method = unsafeClass.getDeclaredMethod("pageSize");
            method.setAccessible(true);
            return (int) (Integer) method.invoke(theUnsafe.get(null));
        } catch (Throwable ignore) {
        }
        return 4096;
    }

    static boolean makeFileIfNotExist(File file) throws IOException {
        if (file.isFile()) {
            return true;
        } else {
            File parent = file.getParentFile();
            return parent != null && (parent.isDirectory() || parent.mkdirs()) && file.createNewFile();
        }
    }

    static byte[] getBytes(File file) throws IOException {
        if (!file.isFile()) {
            return null;
        }
        long len = file.length();
        if ((len >> 32) != 0) {
            throw new IllegalArgumentException("file too large, path:" + file.getPath());
        }
        byte[] bytes = new byte[(int) len];
        readBytes(file, bytes, (int) len);
        return bytes;
    }

    static void readBytes(File file, byte[] bytes, int len) throws IOException {
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        try {
            int p = 0;
            while (p < len) {
                int count = accessFile.read(bytes, p, len - p);
                if (count < 0) break;
                p += count;
            }
        } finally {
            closeQuietly(accessFile);
        }
    }

    static boolean saveBytes(File file, byte[] bytes) {
        try {
            File tmpFile = new File(file.getParent(), file.getName() + ".tmp");
            if (!makeFileIfNotExist(tmpFile)) {
                return false;
            }
            RandomAccessFile accessFile = new RandomAccessFile(tmpFile, "rw");
            try {
                accessFile.setLength(bytes.length);
                accessFile.write(bytes);
            } finally {
                closeQuietly(accessFile);
            }
            if (!file.exists() || file.delete()) {
                return tmpFile.renameTo(file);
            }
        } catch (Exception ignore) {
        }
        return false;
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void deleteFile(File file) {
        try {
            if (file.exists()) {
                deleteRecursive(file);
            }
        } catch (Exception ignore) {
        }
    }

    private static void deleteRecursive(File file) {
        if (file.isDirectory()) {
            File[] fs = file.listFiles();
            if (fs != null) {
                for (File f : fs) {
                    deleteRecursive(f);
                }
            }
        }
        //noinspection ResultOfMethodCallIgnored
        file.delete();
    }

    static int binarySearch(int[] a, int value) {
        int lo = 0;
        int hi = (a.length >> 1) - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final int midVal = a[mid << 1];
            if (midVal < value) {
                lo = mid + 1;
            } else if (midVal > value) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return hi;
    }
}
