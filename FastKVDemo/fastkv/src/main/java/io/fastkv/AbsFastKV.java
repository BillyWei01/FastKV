package io.fastkv;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("rawtypes")
abstract class AbsFastKV {
    protected static final String BOTH_FILES_ERROR = "both files error";
    protected static final String PARSE_DATA_FAILED = "parse dara failed";
    protected static final String OPEN_FILE_FAILED = "open file failed";
    protected static final String MAP_FAILED = "map failed";

    static final String TRUNCATE_FINISH = "truncate finish";
    static final String GC_FINISH = "gc finish";

    protected static final String A_SUFFIX = ".kva";
    protected static final String B_SUFFIX = ".kvb";
    protected static final String C_SUFFIX = ".kvc";
    protected static final String TEMP_SUFFIX = ".tmp";

    protected static final int DATA_SIZE_LIMIT = 1 << 29;

    protected static final int[] TYPE_SIZE = {0, 1, 4, 4, 8, 8};
    protected static final byte[] EMPTY_ARRAY = new byte[0];
    protected static final int DATA_START = 12;
    protected static final int BASE_GC_KEYS_THRESHOLD = 80;
    protected static final int BASE_GC_BYTES_THRESHOLD = 4096;
    protected static final int INTERNAL_LIMIT = 2048;

    protected static final int PAGE_SIZE = Util.getPageSize();
    protected static final int DOUBLE_LIMIT = Math.max(PAGE_SIZE << 1, 1 << 14);
    protected static final int TRUNCATE_THRESHOLD = DOUBLE_LIMIT << 1;

    protected final String path;
    protected final String name;
    protected final Map<String, FastKV.Encoder> encoderMap;
    protected final FastKV.Logger logger = FastKVConfig.sLogger;

    protected int dataEnd;
    protected long checksum;
    protected final HashMap<String, Container.BaseContainer> data = new HashMap<>();
    protected boolean startLoading = false;

    protected FastBuffer fastBuffer;

    protected String tempExternalName;

    protected int invalidBytes;
    protected final ArrayList<Segment> invalids = new ArrayList<>();

    protected AbsFastKV(final String path, final String name, FastKV.Encoder[] encoders) {
        this.path = path;
        this.name = name;
        Map<String, FastKV.Encoder> map = new HashMap<>();
        StringSetEncoder encoder = StringSetEncoder.INSTANCE;
        map.put(encoder.tag(), encoder);
        if (encoders != null && encoders.length > 0) {
            for (FastKV.Encoder e : encoders) {
                String tag = e.tag();
                if (map.containsKey(tag)) {
                    error("duplicate encoder tag:" + tag);
                } else {
                    map.put(tag, e);
                }
            }
        }
        this.encoderMap = map;
    }

    protected final int getNewCapacity(int capacity, int expected) {
        if (expected > DATA_SIZE_LIMIT) {
            throw new IllegalStateException("data size out of limit");
        }
        if (expected <= PAGE_SIZE) {
            return PAGE_SIZE;
        } else {
            while (capacity < expected) {
                if (capacity <= DOUBLE_LIMIT) {
                    capacity <<= 1;
                } else {
                    capacity += DOUBLE_LIMIT;
                }
            }
            return capacity;
        }
    }

    protected final boolean loadWithBlockingIO(File srcFile) throws IOException {
        long fileLen = srcFile.length();
        if (fileLen == 0 || fileLen > DATA_SIZE_LIMIT) {
            return false;
        }
        int fileSize = (int) fileLen;
        int capacity = getNewCapacity(PAGE_SIZE, fileSize);
        FastBuffer buffer;
        if (fastBuffer != null && fastBuffer.hb.length == capacity) {
            buffer = fastBuffer;
            buffer.position = 0;
        } else {
            buffer = new FastBuffer(new byte[capacity]);
            fastBuffer = buffer;
        }
        Util.readBytes(srcFile, buffer.hb, fileSize);
        int dataSize = buffer.getInt();
        long sum = buffer.getLong();
        dataEnd = DATA_START + dataSize;
        if (dataSize >= 0 && (dataSize <= fileSize - DATA_START)
                && sum == buffer.getChecksum(DATA_START, dataSize)
                && parseData() == 0) {
            checksum = sum;
            return true;
        }
        return false;
    }

    protected final void deleteCFiles() {
        try {
            Util.deleteFile(new File(path, name + C_SUFFIX));
            Util.deleteFile(new File(path, name + TEMP_SUFFIX));
        } catch (Exception e) {
            error(e);
        }
    }

    protected final int parseData() {
        FastBuffer buffer = fastBuffer;
        buffer.position = DATA_START;
        try {
            while (buffer.position < dataEnd) {
                int start = buffer.position;
                byte info = buffer.get();
                byte type = (byte) (info & DataType.TYPE_MASK);
                if (type < DataType.BOOLEAN || type > DataType.OBJECT) {
                    throw new Exception(PARSE_DATA_FAILED);
                }
                int keySize = buffer.get() & 0xFF;
                if (info < 0) {
                    buffer.position += keySize;
                    int valueSize = (type <= DataType.DOUBLE) ? TYPE_SIZE[type] : buffer.getShort() & 0xFFFF;
                    fastBuffer.position += valueSize;
                    countInvalid(start, fastBuffer.position);
                    continue;
                }
                String key = buffer.getString(keySize);
                int pos = buffer.position;
                if (type <= DataType.DOUBLE) {
                    switch (type) {
                        case DataType.BOOLEAN:
                            data.put(key, new Container.BooleanContainer(pos, buffer.get() == 1));
                            break;
                        case DataType.INT:
                            data.put(key, new Container.IntContainer(pos, buffer.getInt()));
                            break;
                        case DataType.LONG:
                            data.put(key, new Container.LongContainer(pos, buffer.getLong()));
                            break;
                        case DataType.FLOAT:
                            data.put(key, new Container.FloatContainer(pos, buffer.getFloat()));
                            break;
                        default:
                            data.put(key, new Container.DoubleContainer(pos, buffer.getDouble()));
                            break;
                    }
                } else {
                    int size = buffer.getShort() & 0xFFFF;
                    boolean external = (info & DataType.EXTERNAL_MASK) != 0;
                    checkValueSize(size, external);
                    switch (type) {
                        case DataType.STRING:
                            String str = buffer.getString(size);
                            data.put(key, new Container.StringContainer(start, pos + 2, str, size, external));
                            break;
                        case DataType.ARRAY:
                            Object value = external ? buffer.getString(size) : buffer.getBytes(size);
                            data.put(key, new Container.ArrayContainer(start, pos + 2, value, size, external));
                            break;
                        default:
                            if (external) {
                                String fileName = buffer.getString(size);
                                data.put(key, new Container.ObjectContainer(start, pos + 2, fileName, size, true));
                            } else {
                                int tagSize = buffer.get() & 0xFF;
                                String tag = buffer.getString(tagSize);
                                FastKV.Encoder encoder = encoderMap.get(tag);
                                int objectSize = size - (tagSize + 1);
                                if (objectSize < 0) {
                                    throw new Exception(PARSE_DATA_FAILED);
                                }
                                if (encoder != null) {
                                    try {
                                        Object obj = encoder.decode(buffer.hb, buffer.position, objectSize);
                                        if (obj != null) {
                                            data.put(key, new Container.ObjectContainer(start, pos + 2, obj, size, false));
                                        }
                                    } catch (Exception e) {
                                        error(e);
                                    }
                                } else {
                                    error("object with tag: " + tag + " without encoder");
                                }
                                buffer.position += objectSize;
                            }
                            break;
                    }
                }
            }
        } catch (Exception e) {
            warning(e);
            return -1;
        }
        if (buffer.position != dataEnd) {
            warning(new Exception(PARSE_DATA_FAILED));
            return -1;
        }
        return 0;
    }

    protected final void checkValueSize(int size, boolean external) {
        if (external) {
            if (size != Util.NAME_SIZE) {
                throw new IllegalStateException("name size not match");
            }
        } else {
            if (size < 0 || size >= INTERNAL_LIMIT) {
                throw new IllegalStateException("value size out of bound");
            }
        }
    }

    protected static class Segment implements Comparable<Segment> {
        int start;
        int end;

        Segment(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public int compareTo(Segment o) {
            return start - o.start;
        }
    }

    protected final void mergeInvalids() {
        int i = invalids.size() - 1;
        Segment p = invalids.get(i);
        while (i > 0) {
            Segment q = invalids.get(--i);
            if (p.start == q.end) {
                q.end = p.end;
                invalids.remove(i + 1);
            }
            p = q;
        }
    }

    protected final void updateOffset(int gcStart, int[] srcToShift) {
        Collection<Container.BaseContainer> values = data.values();
        for (Container.BaseContainer c : values) {
            if (c.offset > gcStart) {
                int index = Util.binarySearch(srcToShift, c.offset);
                int shift = srcToShift[(index << 1) + 1];
                c.offset -= shift;
                if (c.getType() >= DataType.STRING) {
                    ((Container.VarContainer) c).start -= shift;
                }
            }
        }
    }

    protected final void tryBlockingIO(File aFile, File bFile) {
        try {
            if (loadWithBlockingIO(aFile)) {
                return;
            }
        } catch (IOException e) {
            warning(e);
        }
        resetMemory();
        try {
            if (loadWithBlockingIO(bFile)) {
                return;
            }
        } catch (IOException e) {
            warning(e);
        }
        resetMemory();
    }


    protected final void resetMemory() {
        resetData();
        resetBuffer();
    }

    protected void resetData() {
        dataEnd = DATA_START;
        checksum = 0L;
        data.clear();
        clearInvalid();
    }

    private void resetBuffer() {
        if (fastBuffer == null || fastBuffer.hb.length != PAGE_SIZE) {
            fastBuffer = new FastBuffer(PAGE_SIZE);
        } else {
            fastBuffer.putInt(0, 0);
            fastBuffer.putLong(4, 0L);
        }
    }

    protected final void countInvalid(int start, int end) {
        invalidBytes += (end - start);
        invalids.add(new Segment(start, end));
    }

    protected final void clearInvalid() {
        invalidBytes = 0;
        invalids.clear();
    }

    protected final void error(String message) {
        if (logger != null) {
            logger.e(name, new Exception(message));
        }
    }

    protected final void error(Exception e) {
        if (logger != null) {
            logger.e(name, e);
        }
    }

    protected final void warning(Exception e) {
        if (logger != null) {
            logger.w(name, e);
        }
    }

    protected final void info(String message) {
        if (logger != null) {
            logger.i(name, message);
        }
    }

    protected final int bytesThreshold() {
        if (dataEnd <= (1 << 14)) {
            return BASE_GC_BYTES_THRESHOLD;
        } else {
            return dataEnd <= (1 << 16) ? BASE_GC_BYTES_THRESHOLD << 1 : BASE_GC_BYTES_THRESHOLD << 2;
        }
    }

    protected final long shiftCheckSum(long checkSum, int offset) {
        int shift = (offset & 7) << 3;
        return (checkSum << shift) | (checkSum >>> (64 - shift));
    }

    public synchronized boolean contains(String key) {
        return data.containsKey(key);
    }

    public synchronized boolean getBoolean(String key) {
        return getBoolean(key, false);
    }

    public synchronized boolean getBoolean(String key, boolean defValue) {
        Container.BooleanContainer c = (Container.BooleanContainer) data.get(key);
        return c == null ? defValue : c.value;
    }

    public int getInt(String key) {
        return getInt(key, 0);
    }

    public synchronized int getInt(String key, int defValue) {
        Container.IntContainer c = (Container.IntContainer) data.get(key);
        return c == null ? defValue : c.value;
    }

    public float getFloat(String key) {
        return getFloat(key, 0f);
    }

    public synchronized float getFloat(String key, float defValue) {
        Container.FloatContainer c = (Container.FloatContainer) data.get(key);
        return c == null ? defValue : c.value;
    }

    public synchronized long getLong(String key) {
        Container.LongContainer c = (Container.LongContainer) data.get(key);
        return c == null ? 0L : c.value;
    }

    public synchronized long getLong(String key, long defValue) {
        Container.LongContainer c = (Container.LongContainer) data.get(key);
        return c == null ? defValue : c.value;
    }

    public double getDouble(String key) {
        return getDouble(key, 0D);
    }

    public synchronized double getDouble(String key, double defValue) {
        Container.DoubleContainer c = (Container.DoubleContainer) data.get(key);
        return c == null ? defValue : c.value;
    }

    public String getString(String key) {
        return getString(key, "");
    }

    public synchronized String getString(String key, String defValue) {
        Container.StringContainer c = (Container.StringContainer) data.get(key);
        if (c != null) {
            return c.external ? getStringFromFile(c) : (String) c.value;
        }
        return defValue;
    }

    private String getStringFromFile(Container.StringContainer c) {
        String fileName = (String) c.value;
        File file = new File(path + name, fileName);
        try {
            byte[] bytes = Util.getBytes(file);
            if (bytes != null) {
                return (bytes.length == 0) ? "" : new String(bytes, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            error(e);
        }
        return "";
    }

    public byte[] getArray(String key) {
        return getArray(key, EMPTY_ARRAY);
    }

    public synchronized byte[] getArray(String key, byte[] defValue) {
        Container.ArrayContainer c = (Container.ArrayContainer) data.get(key);
        if (c != null) {
            return c.external ? getArrayFromFile(c) : (byte[]) c.value;
        }
        return defValue;
    }

    private byte[] getArrayFromFile(Container.ArrayContainer c) {
        File file = new File(path + name, (String) c.value);
        try {
            byte[] a = Util.getBytes(file);
            return a != null ? a : EMPTY_ARRAY;
        } catch (Exception e) {
            error(e);
        }
        return EMPTY_ARRAY;
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> T getObject(String key) {
        Container.ObjectContainer c = (Container.ObjectContainer) data.get(key);
        if (c != null) {
            return c.external ? (T) getObjectFromFile(c) : (T) c.value;
        }
        return null;
    }

    private Object getObjectFromFile(Container.ObjectContainer c) {
        File file = new File(path + name, (String) c.value);
        try {
            byte[] bytes = Util.getBytes(file);
            if (bytes != null) {
                int tagSize = bytes[0] & 0xFF;
                String tag = new String(bytes, 1, tagSize, StandardCharsets.UTF_8);
                FastKV.Encoder encoder = encoderMap.get(tag);
                if (encoder != null) {
                    int offset = 1 + tagSize;
                    return encoder.decode(bytes, offset, bytes.length - offset);
                } else {
                    warning(new Exception("No encoder for tag:" + tag));
                }
            } else {
                warning(new Exception("Read object data failed"));
            }
        } catch (Exception e) {
            error(e);
        }
        return null;
    }

    public synchronized Set<String> getStringSet(String key) {
        return getObject(key);
    }

    public synchronized Map<String, Object> getAll() {
        int size = data.size();
        if (size == 0) {
            return new HashMap<>();
        }
        Map<String, Object> result = new HashMap<>(size * 4 / 3 + 1);
        for (Map.Entry<String, Container.BaseContainer> entry : data.entrySet()) {
            String key = entry.getKey();
            Container.BaseContainer c = entry.getValue();
            Object value = null;
            switch (c.getType()) {
                case DataType.BOOLEAN:
                    value = ((Container.BooleanContainer) c).value;
                    break;
                case DataType.INT:
                    value = ((Container.IntContainer) c).value;
                    break;
                case DataType.FLOAT:
                    value = ((Container.FloatContainer) c).value;
                    break;
                case DataType.LONG:
                    value = ((Container.LongContainer) c).value;
                    break;
                case DataType.DOUBLE:
                    value = ((Container.DoubleContainer) c).value;
                    break;
                case DataType.STRING:
                    Container.StringContainer sc = (Container.StringContainer) c;
                    value = sc.external ? getStringFromFile(sc) : sc.value;
                    break;
                case DataType.ARRAY:
                    Container.ArrayContainer ac = (Container.ArrayContainer) c;
                    value = ac.external ? getArrayFromFile(ac) : ac.value;
                    break;
                case DataType.OBJECT:
                    Container.ObjectContainer oc = (Container.ObjectContainer) c;
                    value = oc.external ? getObjectFromFile(oc) : ((Container.ObjectContainer) c).value;
                    break;
            }
            result.put(key, value);
        }
        return result;
    }
}

