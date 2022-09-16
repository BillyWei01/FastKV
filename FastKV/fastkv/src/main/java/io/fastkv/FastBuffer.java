package io.fastkv;

import java.nio.charset.StandardCharsets;

class FastBuffer {
    private static final int MAX_CHAR_LEN = 2048;
    private static final String INVALID_STRING = "Invalid String";

    private char[] charBuf = null;

    public byte[] hb;
    public int position;

    public FastBuffer(int capacity) {
        this(new byte[capacity], 0);
    }

    public FastBuffer(byte[] array) {
        this(array, 0);
    }

    public FastBuffer(byte[] array, int offset) {
        hb = array;
        position = offset;
    }

    public byte get() {
        return hb[position++];
    }

    public final void put(byte v) {
        hb[position++] = v;
    }

    public short getShort() {
        return (short) (((hb[position++] & 0xFF)) |
                ((hb[position++]) << 8));
    }

    public void putShort(short v) {
        hb[position++] = (byte) v;
        hb[position++] = (byte) (v >> 8);
    }

    public int getInt() {
        return (((hb[position++] & 0xFF)) |
                ((hb[position++] & 0xFF) << 8) |
                ((hb[position++] & 0xFF) << 16) |
                ((hb[position++]) << 24));
    }

    public void putInt(int v) {
        hb[position++] = (byte) v;
        hb[position++] = (byte) (v >> 8);
        hb[position++] = (byte) (v >> 16);
        hb[position++] = (byte) (v >> 24);
    }

    public void putInt(int i, int v) {
        hb[i++] = (byte) v;
        hb[i++] = (byte) (v >> 8);
        hb[i++] = (byte) (v >> 16);
        hb[i] = (byte) (v >> 24);
    }

    public int getVarint32() {
        int x = hb[position++];
        if ((x >> 7) == 0) return x;
        x = (x & 0x7f) | (hb[position++] << 7);
        if ((x >> 14) == 0) return x;
        x = (x & 0x3fff) | (hb[position++] << 14);
        if ((x >> 21) == 0) return x;
        x = (x & 0x1fffff) | (hb[position++] << 21);
        if ((x >> 28) == 0) return x;
        x = (x & 0xfffffff) | (hb[position++] << 28);
        return x;
    }

    public int putVarint32(int i, int v) {
        while ((v & 0xffffff80) != 0) {
            hb[i++] = (byte) ((v & 0x7f) | 0x80);
            v >>>= 7;
        }
        hb[i++] = (byte) v;
        return i;
    }

    public void putVarint32(int v) {
        position = putVarint32(position, v);
    }

    public static int getVarint32Size(int v) {
        if ((v >> 7) == 0) {
            return 1;
        } else if ((v >> 14) == 0) {
            return 2;
        } else if ((v >> 21) == 0) {
            return 3;
        } else if ((v >> 28) == 0) {
            return 4;
        }
        return 5;
    }

    public void putLong(int i, long v) {
        hb[i++] = (byte) v;
        hb[i++] = (byte) (v >> 8);
        hb[i++] = (byte) (v >> 16);
        hb[i++] = (byte) (v >> 24);
        hb[i++] = (byte) (v >> 32);
        hb[i++] = (byte) (v >> 40);
        hb[i++] = (byte) (v >> 48);
        hb[i] = (byte) (v >> 56);
    }

    public void putLong(long v) {
        putLong(position, v);
        position += 8;
    }

    public long getLong(int i) {
        return (((long) hb[i++] & 0xFF) |
                (((long) hb[i++] & 0xFF) << 8) |
                (((long) hb[i++] & 0xFF) << 16) |
                (((long) hb[i++] & 0xFF) << 24) |
                (((long) hb[i++] & 0xFF) << 32) |
                (((long) hb[i++] & 0xFF) << 40) |
                (((long) hb[i++] & 0xFF) << 48) |
                (((long) hb[i]) << 56));
    }

    public long getLong() {
        long value = getLong(position);
        position += 8;
        return value;
    }

    public float getFloat() {
        return Float.intBitsToFloat(getInt());
    }

    public double getDouble() {
        return Double.longBitsToDouble(getLong());
    }

    public byte[] getBytes(int len) {
        byte[] bytes = new byte[len];
        System.arraycopy(hb, position, bytes, 0, len);
        position += len;
        return bytes;
    }

    public void putBytes(byte[] src) {
        int len = src.length;
        if (len > 0) {
            System.arraycopy(src, 0, hb, position, len);
            position += len;
        }
    }

    public String getString(int len) {
        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        } else {
            String str = len > MAX_CHAR_LEN ? new String(hb, position, len, StandardCharsets.UTF_8) : decodeStr(len);
            position += len;
            return str;
        }
    }

    public void putString(String str) {
        if (str != null && !str.isEmpty()) {
            encodeStr(str);
        }
    }

    /**
     * get string size in utf-8
     *
     * @param str The String
     * @return The utf-8 size of the string
     */
    public static int getStringSize(String str) {
        int j = 0;
        int i = 0;
        int n = str.length();
        while (i < n) {
            char c = str.charAt(i++);
            if (c < 0x80) {
                j += 1;
            } else if (c < 0x800) {
                j += 2;
            } else if ((c < 0xD800 || c > 0xDFFF)) {
                j += 3;
            } else {
                i++;
                j += 4;
            }
        }
        return j;
    }

    private char[] getCharBuf(int len) {
        char[] buf = charBuf;
        if (buf == null) {
            if (len <= 256) {
                buf = new char[256];
            } else {
                buf = new char[MAX_CHAR_LEN];
            }
            charBuf = buf;
        } else if (buf.length < len) {
            buf = new char[MAX_CHAR_LEN];
            charBuf = buf;
        }
        return buf;
    }

    private synchronized String decodeStr(int len) {
        char[] buf = getCharBuf(len);
        byte[] src = hb;
        int i = position;
        int j = 0;
        int limit = position + len;
        while (i < limit) {
            byte b1 = src[i++];
            if (b1 > 0) {
                buf[j++] = (char) b1;
            } else if (b1 < (byte) 0xE0) {
                byte b2 = src[i++];
                if (b1 < (byte) 0xC2 || b2 > (byte) 0xBF) {
                    throw new IllegalArgumentException(INVALID_STRING);
                }
                buf[j++] = (char) (((b1 & 0x1F) << 6) | (b2 & 0x3F));
            } else if (b1 < (byte) 0xF0) {
                byte b2 = src[i++];
                byte b3 = src[i++];
                if ((b1 == (byte) 0xE0 && b2 < (byte) 0xA0)
                        || (b1 == (byte) 0xED && b2 >= (byte) 0xA0)
                        || b2 > (byte) 0xBF
                        || b3 > (byte) 0xBF) {
                    throw new IllegalArgumentException(INVALID_STRING);
                }
                buf[j++] = (char) (((b1 & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F));
            } else {
                byte b2 = src[i++];
                byte b3 = src[i++];
                byte b4 = src[i++];
                if (b2 > (byte) 0xBF
                        || (((b1 << 28) + (b2 - (byte) 0x90)) >> 30) != 0
                        || b3 > (byte) 0xBF
                        || b4 > (byte) 0xBF) {
                    throw new IllegalArgumentException(INVALID_STRING);
                }
                int cp = ((b1 & 0x07) << 18) | ((b2 & 0x3F) << 12) | ((b3 & 0x3F) << 6) | (b4 & 0x3F);
                buf[j++] = (char) (0xD7C0 + (cp >>> 10));
                buf[j++] = (char) (0xDC00 + (cp & 0x3FF));
            }
        }
        if (i > limit) {
            throw new IllegalArgumentException(INVALID_STRING);
        }
        return new String(buf, 0, j);
    }

    private void encodeStr(String s) {
        byte[] buf = hb;
        int j = position;
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i++);
            if (c < 0x80) {
                // 0xxxxxxx
                buf[j++] = (byte) c;
            } else if (c < 0x800) {
                // 110xxxxx 10xxxxxx
                buf[j++] = (byte) (0xC0 | (c >>> 6));
                buf[j++] = (byte) (0x80 | (0x3F & c));
            } else if ((c < 0xD800 || c > 0xDFFF)) {
                // 1110xxxx 10xxxxxx 10xxxxxx
                buf[j++] = (byte) (0xE0 | (c >>> 12));
                buf[j++] = (byte) (0x80 | (0x3F & (c >>> 6)));
                buf[j++] = (byte) (0x80 | (0x3F & c));
            } else {
                // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                char c2 = s.charAt(i++);
                int cp = (c << 10) + c2 + 0xFCA02400;
                buf[j++] = (byte) (0xF0 | (cp >>> 18));
                buf[j++] = (byte) (0x80 | (0x3F & (cp >>> 12)));
                buf[j++] = (byte) (0x80 | (0x3F & (cp >>> 6)));
                buf[j++] = (byte) (0x80 | (0x3F & cp));
            }
        }
        position = j;
    }

    long getChecksum(int start, int size) {
        if (size <= 0) return 0L;
        int p = start;
        int n = size >> 3;
        int remain = size & 7;
        long checkSum = 0L;
        for (int i = 0; i < n; i++) {
            checkSum ^= getLong(p);
            p += 8;
        }
        int maxShift = remain << 3;
        for (int i = 0; i < maxShift; i += 8) {
            checkSum ^= ((long) hb[p++] & 0xFF) << i;
        }
        int shift = (start & 7) << 3;
        return (checkSum << shift) | (checkSum >>> (64 - shift));
    }
}
