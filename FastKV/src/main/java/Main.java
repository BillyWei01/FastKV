import io.fastkv.*;
import io.fastkv.TestHelper;

import java.io.IOException;
import java.util.*;

public class Main {
    private static void testKV() throws IOException {
        FastKVConfig.setLogger(TestHelper.logger);

        String path = "./out/";
        String name = "main";
        FastKV.Encoder<?>[] encoders = new FastKV.Encoder[]{LongListEncoder.INSTANCE};
        FastKV kv = new FastKV.Builder(path, name).encoder(encoders).build();

        if (!kv.getBoolean("flag")) {
            setValues(kv);
            kv.putBoolean("flag", true);
        }
        kv.putInt("int_key", kv.getInt("int_key") + 1);

//        KVViewer.printFastKV(kv);
//        HexViewer.printFile(path + name + ".kva", 160);

        FastKV kv2 = new FastKV.Builder(path, name+"2")
                .encoder(encoders)
                .asyncBlocking()
                .build();

        Map<Class, FastKV.Encoder> encoderMap = new HashMap<>();
        encoderMap.put(ArrayList.class, LongListEncoder.INSTANCE);
        kv2.putAll(kv.getAll(), encoderMap);

        KVViewer.printFastKV(kv2);
        HexViewer.printFile(path + name+"2" + ".kvc", 160);

        kv.close();
        kv2.close();
    }

    private static void setValues(FastKV kv) {
        if (!kv.contains("bool_key")) {
            String boolKey = "bool_key";
            kv.putBoolean(boolKey, true);

            String intKey = "int_key";
            kv.putInt(intKey, 100);

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
        }
    }

    public static void main(String[] args) throws Exception {
        //testKV();
        HexViewer.printFile( "/Users/bill/Downloads/user_data.kva", 320);
    }
}
