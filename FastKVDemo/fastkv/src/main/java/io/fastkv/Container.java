package io.fastkv;

import java.util.Arrays;
import java.util.Objects;

/**
 * struct of primary type (boolean/int/float/long/double):
 * [type|keyLen|keyData|value]
 *
 * <p>
 * struct of variable type (string/array/object):
 * [type|keyLen|keyData|valueLen|value]
 *
 * <p>
 * type: 1 byte,
 * keyLen: 1 byte,
 * valueLen: 2 bytes.
 */
class Container {
    static abstract class BaseContainer {
        // The offset record the start of value, not the start of key-value.
        int offset;

        abstract byte getType();

        abstract boolean equalTo(BaseContainer other);
    }

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
        boolean equalTo(BaseContainer other) {
            return other.getType() == DataType.BOOLEAN &&
                    ((BooleanContainer) other).value == this.value;
        }
    }

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
        boolean equalTo(BaseContainer other) {
            return other.getType() == DataType.INT &&
                    ((IntContainer) other).value == this.value;
        }
    }

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
        boolean equalTo(BaseContainer other) {
            return other.getType() == DataType.FLOAT &&
                    ((FloatContainer) other).value == this.value;
        }
    }

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
        boolean equalTo(BaseContainer other) {
            return other.getType() == DataType.LONG &&
                    ((LongContainer) other).value == this.value;
        }
    }

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
        boolean equalTo(BaseContainer other) {
            return other.getType() == DataType.DOUBLE &&
                    ((DoubleContainer) other).value == this.value;
        }
    }

    static abstract class VarContainer extends BaseContainer {
        Object value;
        int start;
        int valueSize;
        boolean external;

        VarContainer(int start, int offset, Object value, int size, boolean external) {
            this.start = start;
            this.offset = offset;
            this.value = value;
            this.valueSize = size;
            this.external = external;
        }
    }

    static class StringContainer extends VarContainer {
        StringContainer(int start, int offset, String value, int size, boolean external) {
            super(start, offset, value, size, external);
        }

        @Override
        byte getType() {
            return DataType.STRING;
        }

        @Override
        boolean equalTo(BaseContainer other) {
            return other.getType() == DataType.STRING &&
                    ((StringContainer) other).external == this.external &&
                    Objects.equals(((StringContainer) other).value, this.value);
        }
    }

    static class ArrayContainer extends VarContainer {

        ArrayContainer(int start, int offset, Object value, int size, boolean external) {
            super(start, offset, value, size, external);
        }

        @Override
        byte getType() {
            return DataType.ARRAY;
        }

        @Override
        boolean equalTo(BaseContainer other) {
            if (other.getType() != DataType.ARRAY) return false;
            Object otherValue = ((ArrayContainer) other).value;
            if (value == otherValue) return true;
            if (value != null && otherValue != null) {
                if (value instanceof String) {
                    return value.equals(otherValue);
                }
                if (value instanceof byte[]) {
                    if (otherValue instanceof byte[]) {
                        return Arrays.equals((byte[]) value, (byte[]) otherValue);
                    } else {
                        return false;
                    }
                }
            }
            return false;
        }
    }

    static class ObjectContainer extends VarContainer {
        ObjectContainer(int start, int offset, Object value, int size, boolean external) {
            super(start, offset, value, size, external);
        }

        @Override
        byte getType() {
            return DataType.OBJECT;
        }

        @Override
        boolean equalTo(BaseContainer other) {
            return other.getType() == DataType.OBJECT &&
                    Objects.equals(((ObjectContainer) other).value, this.value);
        }
    }
}
