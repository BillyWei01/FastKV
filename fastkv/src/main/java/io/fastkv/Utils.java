package io.fastkv;

import android.annotation.SuppressLint;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import io.fastkv.interfaces.FastLogger;

class Utils {
    private static class Holder {
        static final SecureRandom random = new SecureRandom();
        static final char[] digits = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'a', 'b', 'c', 'd', 'e', 'f'};
    }

    static final int NAME_SIZE = 32;
    private static final int DEFAULT_PAGE_SIZE = 16 * 1024;


    /**
     * 类似 UUID.randomUUID().toString()，但不包含 '-'。
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

    @SuppressWarnings({"rawtypes", "unchecked", "ConstantConditions"})
    @SuppressLint("DiscouragedPrivateApi")
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
        return DEFAULT_PAGE_SIZE;
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

    static boolean saveBytes(File dstFile, byte[] bytes, AtomicBoolean canceled) {
        File tmpFile = null;
        try {
            int len = bytes.length;
            tmpFile = new File(dstFile.getParent(), dstFile.getName() + ".tmp");
            if (!makeFileIfNotExist(tmpFile)) {
                logError(new Exception("create file failed"));
                return false;
            }
            if (canceled != null && canceled.get()) {
                return false;
            }
            try (RandomAccessFile accessFile = new RandomAccessFile(tmpFile, "rw")) {
                accessFile.setLength(len);
                accessFile.write(bytes, 0, len);
                if (canceled != null && canceled.get()) {
                    return false;
                }
                accessFile.getFD().sync();
            }
            return renameFile(tmpFile, dstFile);
        } catch (Exception e) {
            logError(new Exception("save bytes failed", e));
        } finally {
            if (canceled != null && canceled.get()) {
                deleteFile(tmpFile);
                deleteFile(dstFile);
            }
        }
        return false;
    }


    static boolean renameFile(File srcFile, File dstFile) {
        if (srcFile.renameTo(dstFile)) {
            return true;
        }
        return (!dstFile.exists() || dstFile.delete()) && srcFile.renameTo(dstFile);
    }

    static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable ignore) {
            }
        }
    }

    static void deleteFile(File file) {
        try {
            if (file.exists()) {
                deleteRecursive(file);
            }
        } catch (Throwable ignore) {
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

    static void moveDirFiles(File srcDir, String currentDir) {
        if (srcDir.isDirectory()) {
            File[] fs = srcDir.listFiles();
            if (fs != null) {
                for (File file : fs) {
                    try {
                        Utils.moveFile(file, new File(currentDir, file.getName()));
                    } catch (Exception e) {
                        logError(e);
                    }
                }
            }
        }
    }

    private static void moveFile(File srcFile, File dstFile) throws IOException {
        if (!srcFile.exists() || dstFile.exists()) {
            return;
        }
        if (!srcFile.renameTo(dstFile)) {
            saveBytes(dstFile, getBytes(srcFile), null);
            deleteFile(srcFile);
        }
    }

    static int binarySearch(int[] a, int value) {
        int lo = 0;
        int hi = a.length - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final int midVal = a[mid];
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


    private static void logError(Exception e) {
        FastLogger logger = FastKVConfig.sLogger;
        if (logger != null) {
            logger.e("FastKV", e);
        }
    }
}
