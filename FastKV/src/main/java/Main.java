import io.fastkv.*;
import io.fastkv.TestHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static void testKV() throws IOException {
        FastKVConfig.setLogger(TestHelper.logger);


        String path = "./out/";
        String name = "main";
        FastKV.Encoder<?>[] encoders = new FastKV.Encoder[]{LongListEncoder.INSTANCE};
        FastKV kv = new FastKV.Builder(path, name).encoder(encoders).build();
        //FastKV kv = new FastKV.Builder(path, name).build();
        if(!kv.getBoolean("flag")){
            kv.putBoolean("flag" , true);
        }

        // kv.clear();

        if(!kv.contains("bool_key")){
            String boolKey = "bool_key";
            kv.putBoolean(boolKey, true);

            String intKey = "int_key";
            kv.putInt(intKey, 12345);

            String floatKey = "float_key";
            kv.putFloat(floatKey, 3.1415f);

            String longKey = "long_key";
            kv.putLong(longKey, Long.MAX_VALUE);

            String doubleKey = "double_key";
            kv.putDouble(doubleKey, 99.9);

            String stringKey = "string_key";
            kv.putString(stringKey, "hello, 你好a");

            String stringSetKey = "string_set_key";
            kv.putStringSet(stringSetKey, TestHelper.makeStringSet());

            String objectKey = "long_list";
            List<Long> list = new ArrayList<>();
            list.add(100L);
            list.add(200L);
            list.add(300L);
            kv.putObject(objectKey, list, LongListEncoder.INSTANCE);

            // List<Long> list2 = kv.getObject("long_list");
        }

        KVViewer.printFastKV(kv);
        HexViewer.printFile(path + name + ".kva", 160);
    }

    public static void main(String[] args) throws Exception {
        testKV();
    }
}
