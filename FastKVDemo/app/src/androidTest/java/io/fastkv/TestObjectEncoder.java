package io.fastkv;

import androidx.annotation.NonNull;

import io.fastkv.interfaces.FastEncoder;
import io.packable.PackDecoder;
import io.packable.PackEncoder;

class TestObjectEncoder implements FastEncoder<TestObject> {
    static TestObjectEncoder INSTANCE = new TestObjectEncoder();

    private TestObjectEncoder() {
    }

    @Override
    public String tag() {
        return "TestObject";
    }

    @Override
    public byte[] encode(@NonNull TestObject obj) {
        return PackEncoder.marshal(obj);
    }

    @Override
    public TestObject decode(@NonNull byte[] bytes, int offset, int length) {
        return PackDecoder.unmarshal(bytes, offset, length, TestObject.CREATOR);
    }
}
