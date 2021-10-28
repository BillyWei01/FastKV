package io.fastkv;

import io.packable.PackDecoder;
import io.packable.PackEncoder;

class TestObjectEncoder implements FastKV.Encoder<TestObject> {
    static TestObjectEncoder INSTANCE = new TestObjectEncoder();

    private TestObjectEncoder() {
    }

    @Override
    public String tag() {
        return "TestObject";
    }

    @Override
    public byte[] encode(TestObject obj) {
        return PackEncoder.marshal(obj);
    }

    @Override
    public TestObject decode(byte[] bytes, int offset, int length) {
        return PackDecoder.unmarshal(bytes, offset, length, TestObject.CREATOR);
    }
}
