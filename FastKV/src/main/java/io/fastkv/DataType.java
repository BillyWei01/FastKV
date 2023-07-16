package io.fastkv;

interface DataType {
    byte DELETE_MASK = (byte) 0x80;
    byte EXTERNAL_MASK = 0x40;
    byte TYPE_MASK = 0x3F;

    byte BOOLEAN = 1;
    byte INT = 2;
    byte FLOAT = 3;
    byte LONG = 4;
    byte DOUBLE = 5;
    byte STRING = 6;
    byte ARRAY = 7;
    byte OBJECT = 8;
}
