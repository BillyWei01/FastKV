package io.fastkv;

import io.fastkv.interfaces.FastEncoder;

/**
 * Container类 - 数据容器封装
 * 
 * <p>用于封装不同类型的键值对数据，提供统一的存储结构和类型转换功能。
 * 
 * <h3>存储结构</h3>
 * <ul>
 * <li><b>基本类型</b> (boolean/int/float/long/double): [type|keyLen|keyData|value]</li>
 * <li><b>变长类型</b> (string/array/object): [type|keyLen|keyData|valueLen|value]</li>
 * <li><b>大长度变长类型</b> (string_large/array_large/object_large): [type|keyLen|keyData|valueLen(4字节)|value]</li>
 * </ul>
 *
 * <h3>字段说明</h3>
 * <ul>
 * <li>type: 1字节，数据类型标识</li>
 * <li>keyLen: 1字节，键名长度</li>
 * <li>valueLen: 2字节（普通）或4字节（大长度），值长度</li>
 * </ul>
 * 
 * <h3>类型转换</h3>
 * <p>各Container实现了类型转换方法，支持在不同数据类型间进行转换：
 * <ul>
 * <li>数值类型间可以相互转换</li>
 * <li>布尔值转换：false=0, true=1</li>
 * <li>字符串转换：支持解析为数值或布尔值</li>
 * <li>数组和对象类型使用默认值</li>
 * </ul>
 */
class Container {
    /**
     * 基础容器抽象类
     * 
     * <p>所有数据容器的基类，定义了通用的存储结构和类型转换接口。
     */
    static abstract class BaseContainer {
        /** 值在缓冲区中的偏移量（注意：这是值的偏移量，不是键值对的偏移量） */
        int offset;

        /** 获取数据类型标识 */
        abstract byte getType();

        // 类型转换方法 - 提供默认实现，子类可以覆盖以实现具体的转换逻辑
        
        /** 转换为布尔值，默认返回false */
        boolean toBoolean() { return false; }
        
        /** 转换为整数，默认返回0 */
        int toInt() { return 0; }
        
        /** 转换为长整数，默认返回0L */
        long toLong() { return 0L; }
        
        /** 转换为单精度浮点数，默认返回0.0f */
        float toFloat() { return 0.0f; }
        
        /** 转换为双精度浮点数，默认返回0.0 */
        double toDouble() { return 0.0; }
        
        /** 转换为字符串，默认返回空字符串 */
        String toStringValue() { return ""; }
    }

    /** 布尔值容器 - 存储boolean类型数据，支持与数值和字符串类型的转换 */
    static class BooleanContainer extends BaseContainer {
        boolean value;

        BooleanContainer(int offset, boolean value) {
            this.offset = offset;
            this.value = value;
        }

        @Override
        byte getType() {
            return DataType.BOOLEAN;
        }

        @Override
        boolean toBoolean() {
            return value;
        }

        @Override
        int toInt() {
            return value ? 1 : 0;
        }

        @Override
        long toLong() {
            return value ? 1L : 0L;
        }

        @Override
        float toFloat() {
            return value ? 1.0f : 0.0f;
        }

        @Override
        double toDouble() {
            return value ? 1.0 : 0.0;
        }

        @Override
        String toStringValue() {
            return value ? "true" : "false";
        }
    }

    /** 整数容器 - 存储int类型数据，支持与其他数值类型和字符串的转换 */
    static class IntContainer extends BaseContainer {
        int value;

        IntContainer(int offset, int value) {
            this.offset = offset;
            this.value = value;
        }

        @Override
        byte getType() {
            return DataType.INT;
        }

        @Override
        boolean toBoolean() {
            return value != 0;
        }

        @Override
        int toInt() {
            return value;
        }

        @Override
        long toLong() {
            return value;
        }

        @Override
        float toFloat() {
            return (float) value;
        }

        @Override
        double toDouble() {
            return value;
        }

        @Override
        String toStringValue() {
            return String.valueOf(value);
        }
    }

    /** 单精度浮点数容器 - 存储float类型数据，支持与其他数值类型和字符串的转换 */
    static class FloatContainer extends BaseContainer {
        float value;

        FloatContainer(int offset, float value) {
            this.offset = offset;
            this.value = value;
        }

        @Override
        byte getType() {
            return DataType.FLOAT;
        }

        @Override
        boolean toBoolean() {
            return value != 0.0f;
        }

        @Override
        int toInt() {
            return (int) value;
        }

        @Override
        long toLong() {
            return (long) value;
        }

        @Override
        float toFloat() {
            return value;
        }

        @Override
        double toDouble() {
            return value;
        }

        @Override
        String toStringValue() {
            return String.valueOf(value);
        }
    }

    /** 长整数容器 - 存储long类型数据，支持与其他数值类型和字符串的转换 */
    static class LongContainer extends BaseContainer {
        long value;

        LongContainer(int offset, long value) {
            this.offset = offset;
            this.value = value;
        }

        @Override
        byte getType() {
            return DataType.LONG;
        }

        @Override
        boolean toBoolean() {
            return value != 0L;
        }

        @Override
        int toInt() {
            return (int) value;
        }

        @Override
        long toLong() {
            return value;
        }

        @Override
        float toFloat() {
            return (float) value;
        }

        @Override
        double toDouble() {
            return (double) value;
        }

        @Override
        String toStringValue() {
            return String.valueOf(value);
        }
    }

    /** 双精度浮点数容器 - 存储double类型数据，支持与其他数值类型和字符串的转换 */
    static class DoubleContainer extends BaseContainer {
        double value;

        DoubleContainer(int offset, double value) {
            this.offset = offset;
            this.value = value;
        }

        @Override
        byte getType() {
            return DataType.DOUBLE;
        }

        @Override
        boolean toBoolean() {
            return value != 0.0;
        }

        @Override
        int toInt() {
            return (int) value;
        }

        @Override
        long toLong() {
            return (long) value;
        }

        @Override
        float toFloat() {
            return (float) value;
        }

        @Override
        double toDouble() {
            return value;
        }

        @Override
        String toStringValue() {
            return String.valueOf(value);
        }
    }

    /** 
     * 变长数据容器抽象类
     * 
     * <p>用于存储变长类型数据（string/array/object），包含额外的元数据信息。
     */
    static abstract class VarContainer extends BaseContainer {
        /** 存储的值对象 */
        Object value;
        /** 键值对在缓冲区中的起始位置 */
        int start;
        /** 值的字节长度 */
        int valueSize;
        /** 是否存储在外部文件中（向前兼容字段） */
        boolean external;

        VarContainer(int start, int offset, Object value, int size, boolean external) {
            this.start = start;
            this.offset = offset;
            this.value = value;
            this.valueSize = size;
            this.external = external;
        }
    }

    /** 
     * 字符串容器 - 存储String类型数据
     * 
     * <p>支持字符串到其他类型的转换，包括数值解析和布尔值识别。
     */
    static class StringContainer extends VarContainer {
        StringContainer(int start, int offset, String value, int size, boolean external) {
            super(start, offset, value, size, external);
        }

        @Override
        byte getType() {
            return DataType.STRING;
        }

        @Override
        boolean toBoolean() {
            return stringToBoolean((String) value);
        }

        @Override
        int toInt() {
            return stringToInt((String) value);
        }

        @Override
        long toLong() {
            return stringToLong((String) value);
        }

        @Override
        float toFloat() {
            return stringToFloat((String) value);
        }

        @Override
        double toDouble() {
            return stringToDouble((String) value);
        }

        @Override
        String toStringValue() {
            return value != null ? (String) value : "";
        }

        // 字符串转换辅助方法 - 实现字符串到其他类型的转换逻辑
        private boolean stringToBoolean(String str) {
            if (str == null) return false;
            return "true".equalsIgnoreCase(str);
        }

        private int stringToInt(String str) {
            if (str == null || str.isEmpty()) return 0;
            
            try {
                // 处理浮点数字符串，截取整数部分
                if (str.contains(".")) {
                    return (int) Double.parseDouble(str);
                }
                return Integer.parseInt(str.trim());
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        private long stringToLong(String str) {
            if (str == null || str.isEmpty()) return 0L;
            
            try {
                // 处理浮点数字符串，截取整数部分
                if (str.contains(".")) {
                    return (long) Double.parseDouble(str);
                }
                return Long.parseLong(str.trim());
            } catch (NumberFormatException e) {
                return 0L;
            }
        }

        private float stringToFloat(String str) {
            if (str == null || str.isEmpty()) return 0.0f;
            
            try {
                return Float.parseFloat(str.trim());
            } catch (NumberFormatException e) {
                return 0.0f;
            }
        }

        private double stringToDouble(String str) {
            if (str == null || str.isEmpty()) return 0.0;
            
            try {
                return Double.parseDouble(str.trim());
            } catch (NumberFormatException e) {
                return 0.0;
        }
    }
    }

    /** 
     * 数组容器 - 存储byte[]类型数据
     * 
     * <p>使用默认的类型转换实现，转换为其他类型时返回默认值。
     */
    static class ArrayContainer extends VarContainer {

        ArrayContainer(int start, int offset, Object value, int size, boolean external) {
            super(start, offset, value, size, external);
        }

        @Override
        byte getType() {
            return DataType.ARRAY;
        }
    }

    /** 
     * 对象容器 - 存储序列化对象数据
     * 
     * <p>使用默认的类型转换实现，转换为其他类型时返回默认值。
     * 包含编码器信息用于对象的序列化和反序列化。
     */
    static class ObjectContainer extends VarContainer {
        /** 用于对象序列化和反序列化的编码器 */
        @SuppressWarnings("rawtypes")
        FastEncoder encoder;

        ObjectContainer(int start, int offset, Object value, int size, boolean external) {
            super(start, offset, value, size, external);
        }

        @Override
        byte getType() {
            return DataType.OBJECT;
        }
    }
}
