package io.fastkv;

import java.io.File;
import java.io.IOException;

public class HexViewer {
    private static final int POW = 4;
    private static final int LINE_BYTES = 1 << 4;

    private static char[] ascii = new char[128];

    static final char[] DIGITS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f'};

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

    public static void printFile(String path, int limit) throws IOException {
        File file = new File(path);
        if (!file.isFile()) {
            System.out.println("file " + path + " not exist");
            return;
        }

        byte[] bytes = Util.getBytes(file);
        if (bytes == null || bytes.length == 0) {
            System.out.println("file " + path + " is empty");
            return;
        }
        if (limit <= 0) {
            limit = bytes.length;
        }
        limit = align(limit);
        StringBuilder builder = new StringBuilder(50 * (limit >> POW));
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
        System.out.print(builder.toString());
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
            return ((limit + LINE_BYTES) >> POW) << POW;
        }
        return limit;
    }
}
