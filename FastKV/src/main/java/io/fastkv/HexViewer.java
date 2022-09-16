package io.fastkv;

import java.io.File;
import java.io.IOException;

public class HexViewer {
    private static final int LINE_BYTES = 16;

    private static final char[] ascii = new char[128];

    static final char[] DIGITS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'A', 'B', 'V', 'D', 'E', 'F'};

    static {
        for (char i = '0'; i <= '9'; i++) {
            ascii[i] = i;
        }
        for (char i = 'a'; i <= 'z'; i++) {
            ascii[i] = i;
        }
        for (char i = 'A'; i <= 'Z'; i++) {
            ascii[i] = i;
        }
        String charSet = "~!@$%^&*()_+-={}[]|\\:;\"'<>,.?/";
        char[] a = charSet.toCharArray();
        for (char ch : a) {
            ascii[ch] = ch;
        }
    }

    static byte[] getBytes(File file, int length) throws IOException {
        if (!file.isFile()) {
            return null;
        }
        long readCount = Math.min(length, file.length());
        if ((readCount >> 32) != 0) {
            throw new IllegalArgumentException("file too large, path:" + file.getPath());
        }
        byte[] bytes = new byte[(int) length];
        Util.readBytes(file, bytes, (int) readCount);
        return bytes;
    }

    public static void printFile(String path, int limit) throws IOException {
        File file = new File(path);
        if (!file.isFile()) {
            System.out.println("file " + path + " not exist");
            return;
        }
        if (limit <= 0) {
            System.out.println("limit <= 0");
            return;
        }
        limit = align(limit);

        byte[] bytes = getBytes(file, limit);
        if (bytes == null || bytes.length == 0) {
            System.out.println("file " + path + " is empty");
            return;
        }
        printBytes(bytes);
    }

    public static void printBytes(byte[] bytes) {
        int limit = align(bytes.length);
        if (bytes.length < limit) {
            System.out.println("extend size to: " + limit);
            byte[] newBytes = new byte[limit];
            System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
            bytes = newBytes;
        }
        StringBuilder builder = new StringBuilder(50 * (limit / LINE_BYTES));
        for (int i = 0; i < limit; i += LINE_BYTES) {
            builder.append(int2Hex(i)).append(':');
            for (int j = i, end = i + LINE_BYTES; j < end; j++) {
                byte b = bytes[j];
                builder.append(' ')
                        .append(DIGITS[(b >> 4) & 0xF])
                        .append(DIGITS[b & 0xF]);
                if (((j + 1) & 7) == 0) {
                    builder.append(' ');
                }
            }
            builder.append(' ').append("\t");
            for (int j = i, end = i + LINE_BYTES; j < end; j++) {
                byte b = bytes[j];
                builder.append((b & 0x80) == 0 && ascii[b] != 0 ? ascii[b] : '.');
            }
            builder.append('\n');
        }
        System.out.print(builder);
    }


    static String int2Hex(int a) {
        char[] buf = new char[8];
        for (int i = 3; i >= 0; i--) {
            int b = a;
            int index = i << 1;
            buf[index] = DIGITS[(b >> 4) & 0xF];
            buf[index + 1] = DIGITS[b & 0xF];
            a = a >>> 8;
        }
        return new String(buf);
    }

    private static int align(int limit) {
        int mask = LINE_BYTES - 1;
        if ((limit & mask) != 0) {
            return ((limit + LINE_BYTES) / LINE_BYTES) * LINE_BYTES;
        }
        return limit;
    }
}
