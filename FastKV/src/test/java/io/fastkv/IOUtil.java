package io.fastkv;

import java.io.*;

public class IOUtil {
    public static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] bytes = new byte[4096];
        int count;
        try {
            while ((count = in.read(bytes)) > 0) {
                out.write(bytes, 0, count);
            }
        }finally {
            closeQuietly(in);
            closeQuietly(out);
        }
    }

    public static byte[] streamToBytes(InputStream in) throws IOException {
        int len = Math.max(in.available(), 1024);
        ByteArrayOutputStream out = new ByteArrayOutputStream(len);
        copy(in, out);
        return out.toByteArray();
    }


    private static boolean makeFileIfNotExits(File file) throws IOException {
        if (file.isFile()) {
            return true;
        } else {
            File parent = file.getParentFile();
            return parent != null && (parent.isDirectory() || parent.mkdirs()) && file.createNewFile();
        }
    }

    public static void bytesToFile(byte[] bytes, File file) throws IOException {
        if(!makeFileIfNotExits(file)){
            return;
        }
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        try {
            accessFile.setLength(0);
            accessFile.write(bytes);
        }finally {
            closeQuietly(accessFile);
        }
    }
}
