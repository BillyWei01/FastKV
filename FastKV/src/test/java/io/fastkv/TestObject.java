package io.fastkv;

import io.packable.PackCreator;
import io.packable.PackEncoder;
import io.packable.Packable;

import java.util.Objects;

class TestObject implements Packable {
   long id;
   String info;

    TestObject(long id, String info){
        this.id = id;
        this.info = info;
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
        return "TestObject{" + "id=" + id + ", info='" + info + '\'' + '}';
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
