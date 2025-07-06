package io.fastkv;

import android.annotation.SuppressLint;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import io.fastkv.interfaces.FastLogger;

class Utils {
    static final int NAME_SIZE = 32;
    private static final int DEFAULT_PAGE_SIZE = 16 * 1024;

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

    static void saveBytes(File dstFile, byte[] bytes, AtomicBoolean canceled) {
        File tmpFile = null;
        try {
            int len = bytes.length;
            tmpFile = new File(dstFile.getParent(), dstFile.getName() + ".tmp");
            if (!makeFileIfNotExist(tmpFile)) {
                logError(new Exception("create file failed"));
                return;
            }
            if (canceled != null && canceled.get()) {
                return;
            }
            try (RandomAccessFile accessFile = new RandomAccessFile(tmpFile, "rw")) {
                accessFile.setLength(len);
                accessFile.write(bytes, 0, len);
                if (canceled != null && canceled.get()) {
                    return;
                }
                accessFile.getFD().sync();
            }
            renameFile(tmpFile, dstFile);
        } catch (Exception e) {
            logError(new Exception("save bytes failed", e));
        } finally {
            if (canceled != null && canceled.get()) {
                deleteFile(tmpFile);
                deleteFile(dstFile);
            }
        }
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
