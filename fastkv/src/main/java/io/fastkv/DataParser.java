package io.fastkv;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.fastkv.Container.*;
import io.fastkv.interfaces.FastCipher;
import io.fastkv.interfaces.FastEncoder;

/**
 * 数据解析器，使用扩展方法模式处理FastKV的数据解析。
 * 类似Kotlin的扩展方法，第一个参数总是FastKV实例。
 */
class DataParser {
    
    private static final String PARSE_DATA_FAILED = "parse dara failed";
    private static final String MISS_CIPHER = "miss cipher";
    
    /**
     * 解析数据 - 扩展方法模式
     * 
     * @param kv FastKV实例
     * @param hadEncrypted 数据是否已加密
     * @return 是否解析成功
     */
    static boolean parseData(FastKV kv, boolean hadEncrypted) {
        if (hadEncrypted && kv.cipher == null) {
            LoggerHelper.error(kv, MISS_CIPHER);
            return false;
        }
        FastCipher dataCipher = hadEncrypted ? kv.cipher : null;
        FastBuffer buffer = kv.fastBuffer;
        buffer.position = FastKV.DATA_START;
        try {
            while (buffer.position < kv.dataEnd) {
                int start = buffer.position;
                byte info = buffer.get();
                byte type = (byte) (info & DataType.TYPE_MASK);
                if (type < DataType.BOOLEAN || type > DataType.OBJECT_LARGE) {
                    throw new Exception(PARSE_DATA_FAILED);
                }
                int keySize = buffer.get() & 0xFF;
                if (keySize == 0) {
                    throw new IllegalStateException("invalid key size");
                }
                if (info < 0) {
                    buffer.position += keySize;
                    int valueSize;
                    if (type <= DataType.DOUBLE) {
                        valueSize = FastKV.TYPE_SIZE[type];
                    } else if (isLargeType(type)) {
                        valueSize = buffer.getInt();
                    } else {
                        valueSize = buffer.getShort() & 0xFFFF;
                    }
                    buffer.position += valueSize;
                    GCHelper.countInvalid(kv, start, buffer.position);
                    continue;
                }
                String key = buffer.getString(dataCipher, keySize);
                int pos = buffer.position;
                if (type <= DataType.DOUBLE) {
                    parseBasicType(kv, buffer, dataCipher, type, key, pos);
                } else {
                    parseComplexType(kv, buffer, dataCipher, type, key, pos, start, info);
                }
            }
        } catch (Exception e) {
            LoggerHelper.error(kv, e);
            return false;
        }
        if (buffer.position != kv.dataEnd) {
            LoggerHelper.error(kv, new Exception(PARSE_DATA_FAILED));
            return false;
        }
        kv.needRewrite = !hadEncrypted && kv.cipher != null && kv.dataEnd != FastKV.DATA_START;
        return true;
    }
    
    /**
     * 解析基本类型数据
     */
    private static void parseBasicType(FastKV kv, FastBuffer buffer, FastCipher dataCipher, 
                                      byte type, String key, int pos) {
        switch (type) {
            case DataType.BOOLEAN:
                kv.data.put(key, new BooleanContainer(pos, buffer.get() == 1));
                break;
            case DataType.INT:
                kv.data.put(key, new IntContainer(pos, buffer.getInt(dataCipher)));
                break;
            case DataType.LONG:
                kv.data.put(key, new LongContainer(pos, buffer.getLong(dataCipher)));
                break;
            case DataType.FLOAT:
                kv.data.put(key, new FloatContainer(pos, buffer.getFloat(dataCipher)));
                break;
            default:
                kv.data.put(key, new DoubleContainer(pos, buffer.getDouble(dataCipher)));
                break;
        }
    }
    
    /**
     * 解析复杂类型数据
     */
    private static void parseComplexType(FastKV kv, FastBuffer buffer, FastCipher dataCipher, 
                                        byte type, String key, int pos, int start, byte info) throws Exception {
        // 根据类型选择长度读取方式
        boolean isLarge = isLargeType(type);
        int size = isLarge ? buffer.getInt() : (buffer.getShort() & 0xFFFF);
        int lengthSize = isLarge ? 4 : 2;

        // 目前不再往写大value到external文件了，但是为了兼容之前的版本，读取时还是判断一下。
        boolean external = (info & DataType.EXTERNAL_MASK) != 0;
        if (external && size != Utils.NAME_SIZE) {
            throw new IllegalStateException("name size not match");
        }
        
        // 将大长度类型映射回普通类型进行处理
        byte normalType = getNormalType(type);
        
        switch (normalType) {
            case DataType.STRING:
                String str = external ? buffer.getString(size) : buffer.getString(dataCipher, size);
                kv.data.put(key, new StringContainer(start, pos + lengthSize, str, size, external));
                break;
            case DataType.ARRAY:
                Object value = external ? buffer.getString(size) : buffer.getBytes(dataCipher, size);
                kv.data.put(key, new ArrayContainer(start, pos + lengthSize, value, size, external));
                break;
            default:
                parseObjectType(kv, buffer, dataCipher, key, pos, start, size, external, lengthSize);
                break;
        }
    }
    
    /**
     * 解析对象类型数据
     */
    private static void parseObjectType(FastKV kv, FastBuffer buffer, FastCipher dataCipher, 
                                       String key, int pos, int start, int size, boolean external, int lengthSize) throws Exception {
        if (external) {
            String fileName = buffer.getString(size);
            kv.data.put(key, new ObjectContainer(start, pos + lengthSize, fileName, size, true));
        } else {
            parseInternalObject(kv, buffer, dataCipher, key, pos, start, size);
            buffer.position = pos + lengthSize + size;
        }
    }
    
    /**
     * 解析内部对象数据
     */
    private static void parseInternalObject(FastKV kv, FastBuffer buffer, FastCipher dataCipher, 
                                           String key, int pos, int start, int size) throws Exception {
        FastBuffer objectBuffer;
        int dataLen;
        if (dataCipher == null) {
            objectBuffer = kv.fastBuffer;
            dataLen = size;
        } else {
            byte[] bytes = new byte[size];
            System.arraycopy(kv.fastBuffer.hb, kv.fastBuffer.position, bytes, 0, size);
            byte[] dstBytes = dataCipher.decrypt(bytes);
            objectBuffer = new FastBuffer(dstBytes);
            dataLen = dstBytes.length;
        }
        int tagSize = objectBuffer.get() & 0xFF;
        String tag = objectBuffer.getString(tagSize);
        //noinspection rawtypes
        FastEncoder encoder = kv.encoderMap.get(tag);
        int objectSize = dataLen - (tagSize + 1);
        if (objectSize < 0) {
            throw new Exception(PARSE_DATA_FAILED);
        }
        if (encoder != null) {
            try {
                Object obj = encoder.decode(objectBuffer.hb, objectBuffer.position, objectSize);
                if (obj != null) {
                    ObjectContainer container = new ObjectContainer(start, pos + 2, obj, size, false);
                    container.encoder = encoder;
                    kv.data.put(key, container);
                }
            } catch (Exception e) {
                LoggerHelper.error(kv, e);
            }
        } else {
            LoggerHelper.error(kv, "object with tag: " + tag + " without encoder");
        }
    }
    
    /**
     * 判断是否为大长度类型
     */
    private static boolean isLargeType(byte type) {
        return type == DataType.STRING_LARGE || type == DataType.ARRAY_LARGE || type == DataType.OBJECT_LARGE;
    }
    
    /**
     * 将大长度类型映射回普通类型
     */
    private static byte getNormalType(byte type) {
        switch (type) {
            case DataType.STRING_LARGE: return DataType.STRING;
            case DataType.ARRAY_LARGE: return DataType.ARRAY;
            case DataType.OBJECT_LARGE: return DataType.OBJECT;
            default: return type;
        }
    }
} 