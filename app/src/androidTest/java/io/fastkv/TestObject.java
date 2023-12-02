package io.fastkv;

import java.util.Objects;

import io.packable.PackCreator;
import io.packable.PackEncoder;
import io.packable.Packable;

class TestObject implements Packable {
    long id;
    String info;

    TestObject(long id, String info) {
        this.id = id;
        this.info = info;
    }

    TestObject copy() {
        return new TestObject(id, info);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TestObject)) return false;
        TestObject object = (TestObject) o;
        return id == object.id &&
                Objects.equals(info, object.info);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, info);
    }

    @Override
    public String toString() {
        return "TestObject{"
                + "id=" + id
                + ", info='" + truncateStr(info)
                + '\'' + '}';
    }

    private String truncateStr(String str) {
        if (str == null) {
            return null;
        }
        if (str.length() > 50) {
            return str.substring(0, 50) + "...";
        }
        return str;
    }

    @Override
    public void encode(PackEncoder encoder) {
        encoder.putLong(0, id)
                .putString(1, info);
    }

    public static final PackCreator<TestObject> CREATOR = decoder -> {
        TestObject obj = new TestObject(decoder.getLong(0), decoder.getString(1));
        decoder.recycle();
        return obj;
    };
}
