package io.fastkv;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import io.fastkv.fastkvdemo.base.AppContext;
import io.fastkv.fastkvdemo.fastkv.LongListEncoder;
import io.fastkv.interfaces.FastEncoder;

public class MPFastKVTest {
    @Before
    public void init() {
        FastKVConfig.setLogger(TestHelper.logger);
    }

    @Test
    public void testPutAndGet() {
        FastEncoder<?>[] encoders = new FastEncoder[]{LongListEncoder.INSTANCE};

        String name = "test_put_and_get";
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name).encoder(encoders).disableWatchFileChange().build();
        kv1.clear();

        String boolKey = "bool_key";
        kv1.putBoolean(boolKey, true);

        String intKey = "int_key";
        kv1.putInt(intKey, 1234);

        String floatKey = "float_key";
        kv1.putFloat(floatKey, 3.14f);

        String longKey = "long_key";
        kv1.putLong(longKey, Long.MAX_VALUE);

        String stringKey = "string_key";
        kv1.putString(stringKey, "hello, 你好");


        String stringSetKey = "string_set_key";
        kv1.putStringSet(stringSetKey, TestHelper.makeStringSet());

        String objectKey = "object_key";
        List<Long> list = new ArrayList<>();
        list.add(-1L);
        list.add(0L);
        list.add(1L);
        list.add(Long.MAX_VALUE);
        kv1.putObject(objectKey, list, LongListEncoder.INSTANCE);

        String doubleKey = "double_key";
        kv1.putDouble(doubleKey, 99.9).commit();

        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, encoders,  null,false);
        Assert.assertEquals(kv1.getBoolean(boolKey), kv2.getBoolean(boolKey));
        Assert.assertEquals(kv1.getInt(intKey), kv2.getInt(intKey));
        Assert.assertEquals(kv1.getFloat(floatKey), kv2.getFloat(floatKey), 0.0);
        Assert.assertEquals(kv1.getLong(longKey), kv2.getLong(longKey));
        Assert.assertEquals(kv1.getDouble(doubleKey), kv2.getDouble(doubleKey), 0.0);
        Assert.assertEquals(kv1.getString(stringKey), kv1.getString(stringKey));
        Assert.assertEquals(kv1.getStringSet(stringSetKey), kv2.getStringSet(stringSetKey));
        Assert.assertTrue(kv1.getObject(objectKey).equals(kv2.getObject(objectKey)));
        Assert.assertNull(kv2.getObject("foo"));
        Assert.assertNull(kv2.getString("foo2", null));
        Assert.assertEquals(kv2.getLong("foo3"), 0);
        Assert.assertTrue(kv2.contains(boolKey));
        Assert.assertFalse(kv2.contains("foo"));
        Assert.assertEquals(100, kv2.getLong("foo_long", 100));

        String name_2 = "put_all";
        //noinspection rawtypes
        Map<Class, FastEncoder> encoderMap = new HashMap<>();
        encoderMap.put(ArrayList.class, LongListEncoder.INSTANCE);
        Map<String, Object> all_1 = kv1.getAll();
        MPFastKV kv3 = new MPFastKV(TestHelper.MP_DIR, name_2, encoders,  null,false);
        kv3.clear();

        kv3.putAll(all_1, encoderMap);
        kv3.commit();

        MPFastKV kv4 = new MPFastKV(TestHelper.MP_DIR, name_2, encoders,  null,false);
        Assert.assertEquals(all_1, kv4.getAll());

        Map<String, Object> m = new HashMap<>();
        m.put("a", "a");
        m.put("b", "b");
        kv3.putAll(m);
        kv3.commit();
        MPFastKV kv5 = new MPFastKV(TestHelper.MP_DIR, name_2, encoders,  null,false);
        Assert.assertEquals("a", kv5.getString("a"));
        Assert.assertEquals("b", kv5.getString("b"));

        subTest(kv1, name, encoders);
    }

    private void subTest(MPFastKV kv1, String name, FastEncoder<?>[] encoders) {
        kv1.putBoolean("b", false);
        kv1.putBoolean("b", true);
        kv1.putInt("i", 100);
        kv1.putInt("i", 200);
        kv1.putLong("L", Long.MAX_VALUE);
        kv1.putLong("L", Long.MIN_VALUE);
        kv1.putFloat("f", 3.14f);
        kv1.putFloat("f", 0.9f);
        kv1.putDouble("D", 1.2D);
        kv1.putDouble("D", 1.5D);
        kv1.putString("S", "Hello");
        kv1.putString("S", "Hello World");

        String arrayKey = "array_key";
        byte[] bytes = arrayKey.getBytes(StandardCharsets.UTF_8);
        kv1.putArray(arrayKey, bytes);
        kv1.putArray(arrayKey, "Hello".getBytes(StandardCharsets.UTF_8));
        kv1.commit();

        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, encoders,  null,false);
        Assert.assertEquals(true, kv2.getBoolean("b"));
        Assert.assertEquals(200, kv2.getInt("i"));
        Assert.assertEquals(Long.MIN_VALUE, kv2.getLong("L"));
        Assert.assertTrue(0.9f == kv2.getFloat("f"));
        Assert.assertTrue(1.5D == kv2.getDouble("D"));
        Assert.assertEquals("Hello World", kv2.getString("S"));
        Assert.assertArrayEquals(kv1.getArray(arrayKey), kv2.getArray(arrayKey));
    }

    @Test
    public void testGC() {
        String name = "test_gc";
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name).disableWatchFileChange().build();
        kv1.clear();

        String longStr = TestHelper.makeString(2000);
        String shortStr = TestHelper.makeString(200);

        int gc1 = TestHelper.gcCount.get();
        kv1.putBoolean("bool_1", true);
        kv1.putInt("int_1", 100);
        kv1.putInt("int_2", 200);
        kv1.putString("short_string", shortStr);
        kv1.putString("string_0", longStr);
        for (int i = 0; i < 10; i++) {
            kv1.putString("string_" + i, longStr);
        }
        for (int i = 0; i < 10; i++) {
            kv1.remove("string_" + i);
        }

        int gc2 = TestHelper.gcCount.get();
        Assert.assertEquals(2, gc2 - gc1);
        for (int i = 0; i < 80; i++) {
            kv1.putString("string_" + i, "hello");
        }
        for (int i = 0; i < 80; i++) {
            kv1.remove("string_" + i);
        }
        int gc3 = TestHelper.gcCount.get();
        Assert.assertEquals(1, gc3 - gc2);
        kv1.commit();

        MPFastKV kvt3 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals(100, kvt3.getInt("int_1"));

        for (int i = 0; i < 10; i++) {
            kv1.putString("string_" + i, longStr);
        }
        String newLongStr = longStr + "hello";
        for (int i = 0; i < 10; i++) {
            kv1.putString("string_" + i, newLongStr);
        }
        kv1.commit();
        int gc4 = TestHelper.gcCount.get();
        Assert.assertEquals(2, gc4 - gc3);
        MPFastKV kvt4 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals(100, kvt4.getInt("int_1"));

        kv1.remove("int_2");
        kv1.commit();

        int truncate1 = TestHelper.truncateCount.get();
        for (int i = 0; i < 30; i++) {
            kv1.putString("long_string_" + i, longStr);
        }
        for (int i = 0; i < 30; i++) {
            kv1.remove("long_string_" + i);
        }
        kv1.commit();
        int truncate2 = TestHelper.truncateCount.get();
        Assert.assertEquals(1, truncate2 - truncate1);

        MPFastKV kv3 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals(100, kv3.getInt("int_1"));
        Assert.assertEquals(0, kv3.getInt("int_2"));
        Assert.assertEquals(kv1.getBoolean("bool_1"), kv3.getBoolean("bool_1"));
        Assert.assertEquals("", kv3.getString("empty_str"));
    }

    @Test
    public void testBigValue() {
        testBigString();
        testBigArray();
        testBigObject();
    }

    private void testBigString() {
        String name = "testBigString";
        clearFile(name);
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name)
                .disableWatchFileChange()
                .build();
        String longStr = TestHelper.makeString(6000);
        kv1.putString("str", longStr);
        kv1.putString("a", "a");
        kv1.putInt("int", 100);
        kv1.commit();

        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals(longStr, kv2.getString("str"));
        Assert.assertEquals(100, kv2.getInt("int"));

        kv1.putString("str", "hello");
        kv1.commit();
        MPFastKV kv3 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals("hello", kv3.getString("str"));

        kv1.putString("str", longStr);
        kv1.commit();
        MPFastKV kv4 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals(longStr, kv4.getString("str"));
    }

    private void testBigArray() {
        String name = "testBigArray";
        clearFile(name);
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name)
                .disableWatchFileChange()
                .build();
        byte[] longArray = new byte[6000];
        kv1.putArray("array", longArray);
        kv1.putString("a", "a");
        kv1.putInt("int", 100);
        kv1.commit();

        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertArrayEquals(longArray, kv2.getArray("array"));
        Assert.assertEquals(100, kv2.getInt("int"));

        byte[] shortArray = "hello".getBytes(StandardCharsets.UTF_8);
        kv1.putArray("array", shortArray);
        kv1.commit();
        MPFastKV kv3 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertArrayEquals(shortArray, kv3.getArray("array"));

        kv1.putArray("array", longArray);
        kv1.commit();
        MPFastKV kv4 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertArrayEquals(longArray, kv4.getArray("array"));
    }

    private void testBigObject() {
        String name = "testBigObject";

        FastEncoder<?>[] encoders = new FastEncoder[]{TestObjectEncoder.INSTANCE};
        MPFastKV kv1 = new MPFastKV(TestHelper.MP_DIR, name, encoders,  null,false);
        kv1.clear();
        String longStr = TestHelper.makeString(6000);
        TestObject obj = new TestObject(12345, longStr);
        kv1.putObject("obj", obj, TestObjectEncoder.INSTANCE);
        kv1.putString("a", "a");
        kv1.putInt("int", 100);
        kv1.commit();

        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, encoders,  null,false);
        Assert.assertEquals(obj, kv2.getObject("obj"));
        Assert.assertEquals(100, kv2.getInt("int"));

        obj.id = 123456;
        obj.info = "hello";
        kv1.putObject("obj", obj, TestObjectEncoder.INSTANCE);
        kv1.commit();
        MPFastKV kv3 = new MPFastKV(TestHelper.MP_DIR, name, encoders,  null,false);
        Assert.assertEquals(obj, kv3.getObject("obj"));

        obj.id = 123457;
        obj.info = longStr;
        kv1.putObject("obj", obj, TestObjectEncoder.INSTANCE);
        kv1.commit();
        MPFastKV kv4 = new MPFastKV(TestHelper.MP_DIR, name, encoders,  null,false);
        Assert.assertEquals(obj, kv4.getObject("obj"));
    }

    private void setValue(String[] values, Random r) {
        String base = TestHelper.makeString(88);
        int n = values.length;
        for (int i = 0; i < n; i++) {
            values[i] = base + r.nextInt();
        }
    }

    @Test
    public void testRewrite() {
        String name = "test_rewrite";
        clearFile(name);
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name).disableWatchFileChange().build();

        //System.currentTimeMillis();
        long seed = 1;
        System.out.println("seed:" + seed);
        Random r = new Random();
        r.setSeed(seed);

        int n = 1000;
        String[] keys = new String[n];
        String[] values = new String[n];
        String keyBase = "test_key_";
        for (int i = 0; i < n; i++) {
            keys[i] = keyBase + i;
        }
        String flag1 = "flag1";
        String flag2 = "flag2";

        for (int i = 0; i < 5; i++) {
            setValue(values, r);
            long t1 = System.nanoTime();
            int gc1 = TestHelper.gcCount.get();
            for (int j = 0; j < n; j++) {
                if (j == 50) {
                    kv1.putString(flag1, "flag1");
                }
                if (j == 100) {
                    kv1.putInt(flag2, 100);
                }
                kv1.putString(keys[j], values[j]);
            }
            kv1.commit();
            long t2 = System.nanoTime();
            int gc2 = TestHelper.gcCount.get();
            System.out.println("use time:" + ((t2 - t1) / 1000000) + ", gc times:" + (gc2 - gc1));
            MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
            Assert.assertEquals("flag1", kv2.getString(flag1));
            Assert.assertEquals(100, kv2.getInt(flag2));
        }
    }

    @Test
    public void testNotASCII() {
        String name = "test_not_ascii";
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name).disableWatchFileChange().build();
        kv1.clear();
        String[] notAscii = new String[]{"\uD842\uDFB7", "一", "二", "三", "四", "五"};
        int i = 1000;
        for (String str : notAscii) {
            kv1.putString(str, str + i);
            i++;
        }
        kv1.commit();
        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        i = 1000;
        for (String str : notAscii) {
            Assert.assertEquals(str + i, kv2.getString(str));
            i++;
        }
        i = 1000;
        for (String str : notAscii) {
            kv1.putString(str, str + (i * i));
            i++;
        }
        i = 1000;
        for (String str : notAscii) {
            Assert.assertEquals(str + i, kv2.getString(str));
            i++;
        }
    }

    @Test
    public void testInRandom() throws IOException {
        String name = "test_in_random";
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name).disableWatchFileChange().build();
        kv1.clear();

        long seed = System.nanoTime();
        System.out.println("random test seed: " + seed);
        Random r = new Random(seed);

        ArrayList<FastKVTest.Pair<String, Object>> srcList = generateInputList(loadSourceData());

        long time = 0;
        List<FastKVTest.Pair<String, Object>> inputList = new ArrayList<>(srcList);
        for (int i = 0; i < 1; i++) {
            long t1 = System.nanoTime();
            putToMPFastKV1(kv1, inputList);
            long t2 = System.nanoTime();
            time += (t2 - t1);

        }
        System.out.println("fill, use time: " + (time / 1000000) + " ms");

        kv1.putString("flag", "hello");
        kv1.commit();

        int round = 3;
        time = 0;
        for (int i = 0; i < round; i++) {
            inputList = getDistributedList(srcList, r, 3);
            long t1 = System.nanoTime();
            putToMPFastKV2(kv1, inputList);
            long t2 = System.nanoTime();
            time += (t2 - t1);
        }
        System.out.println("update, use time: " + (time / 1000000) + " ms");

        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals("hello", kv2.getString("flag"));
    }

    private static void putToMPFastKV1(MPFastKV fastkv, List<FastKVTest.Pair<String, Object>> list) {
        for (FastKVTest.Pair<String, Object> pair : list) {
            String key = pair.first;
            Object value = pair.second;
            if (value instanceof String) {
                fastkv.putString(key, (String) value);
            } else if (value instanceof Boolean) {
                fastkv.putBoolean(key, (Boolean) value);
            } else if (value instanceof Integer) {
                fastkv.putInt(key, (Integer) value);
            } else if (value instanceof Long) {
                fastkv.putLong(key, (Long) value);
            } else if (value instanceof Float) {
                fastkv.putFloat(key, (Float) value);
            } else if (value instanceof Set) {
                //noinspection unchecked
                fastkv.putStringSet(key, (Set<String>) value);
            }
            fastkv.commit();
        }
    }

    private static void putToMPFastKV2(MPFastKV fastkv, List<FastKVTest.Pair<String, Object>> list) {
        for (FastKVTest.Pair<String, Object> pair : list) {
            String key = pair.first;
            Object value = pair.second;
            if (value instanceof String) {
                fastkv.putString(key, (String) value);
            } else if (value instanceof Boolean) {
                fastkv.putBoolean(key, (Boolean) value);
            } else if (value instanceof Integer) {
                fastkv.putInt(key, (Integer) value);
            } else if (value instanceof Long) {
                fastkv.putLong(key, (Long) value);
            } else if (value instanceof Float) {
                fastkv.putFloat(key, (Float) value);
            } else if (value instanceof Set) {
                //noinspection unchecked
                fastkv.putStringSet(key, (Set<String>) value);
            }
        }
        fastkv.commit();
    }

    private static ArrayList<FastKVTest.Pair<String, Object>> generateInputList(Map<String, ?> all) {
        ArrayList<FastKVTest.Pair<String, Object>> list = new ArrayList<>(all.size());
        for (Map.Entry<String, ?> entry : all.entrySet()) {
            list.add(new FastKVTest.Pair<>(entry.getKey(), entry.getValue()));
        }
        return list;
    }

    private Map<String, ?> loadSourceData() throws IOException {
        String srcName = "src";
        File srcFile = new File(TestHelper.MP_DIR, "src.kva");
        if (!srcFile.exists()) {

            InputStream inputStream = AppContext.INSTANCE.getContext().getAssets().open("src.kva");
            //this.getClass().getClassLoader().getResourceAsStream("src.kva");
            if (inputStream == null) {
                throw new IOException("Could not load src.kva");
            }
            byte[] bytes = IOUtil.streamToBytes(inputStream);
            IOUtil.bytesToFile(bytes, srcFile);
        }
        return new MPFastKV.Builder(TestHelper.MP_DIR, srcName).disableWatchFileChange().build().getAll();
    }

    private static Object tuningObject(Object value, Random r) {
        int diff = 2 - r.nextInt(5);
        if (value instanceof String) {
            return makeNewString((String) value, diff);
        } else if (value instanceof Boolean) {
            return diff < 0;
        } else if (value instanceof Integer) {
            return (Integer) value + diff;
        } else if (value instanceof Long) {
            return (Long) value + diff;
        } else if (value instanceof Float) {
            return (Float) value + diff;
        } else if (value instanceof Set) {
            //noinspection unchecked
            Set<String> oldValue = (Set<String>) value;
            Set<String> newValue = new LinkedHashSet<>();
            for (String str : oldValue) {
                newValue.add(makeNewString(str, diff));
            }
            return newValue;
        } else {
            return value;
        }
    }

    private static String generateString(int size) {
        char[] a = new char[size];
        for (int i = 0; i < size; i++) {
            a[i] = (char) ('A' + (i % 26));
        }
        return new String(a);
    }

    private static String makeNewString(String str, int diff) {
        int len = str.length();
        int newLen = Math.max(0, len + diff);
        String newStr;
        if (newLen < len) {
            newStr = str.substring(0, newLen);
        } else if (newLen > len) {
            newStr = str + generateString(newLen - len);
        } else {
            newStr = str.isEmpty() ? "" : str.substring(0, len - 1) + "a";
        }
        return newStr;
    }

    private static List<FastKVTest.Pair<String, Object>> getDistributedList(List<FastKVTest.Pair<String, Object>> srcList, Random r, int n) {
        List<FastKVTest.Pair<String, Object>> inputList = new ArrayList<>(srcList.size());
        int[] a = getDistributedArray(srcList.size(), n, r);
        for (int index : a) {
            FastKVTest.Pair<String, Object> pair = srcList.get(index);
            inputList.add(new FastKVTest.Pair<>(pair.first, tuningObject(pair.second, r)));
        }
        return inputList;
    }

    /**
     * Get array with normal distribution.
     */
    private static int[] getDistributedArray(int n, int times, Random r) {
        int avg = n / 2;
        int v;
        if (n <= 50) {
            v = n;
        } else if (n <= 100) {
            v = (int) (n * 1.5);
        } else if (n <= 200) {
            v = n * 2;
        } else {
            v = n * 3;
        }
        int count = n * times;
        int[] a = new int[count];
        for (int i = 0; i < count; ) {
            int x = (int) (Math.sqrt(v) * r.nextGaussian() + avg);
            if (x >= 0 && x < n) {
                a[i++] = x;
            }
        }
        return a;
    }

    @Test
    public void testForce() throws Exception {
        String name = "test_force";
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name).disableWatchFileChange().build();
        long newTime = System.currentTimeMillis() ^ System.nanoTime();

        kv1.putLong("time", newTime);
        kv1.commit();
        kv1.force();

        File aFile = new File(TestHelper.MP_DIR, name + ".kva");
        RandomAccessFile accessFile = new RandomAccessFile(aFile, "r");
        ByteBuffer buffer = ByteBuffer.allocate(26);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        accessFile.read(buffer.array(), 0, 26);
        long t = buffer.getLong(18);
        Assert.assertEquals(newTime, t);
    }

    private void damageFastKVFile(String fileName) throws IOException {
        File file = new File(TestHelper.MP_DIR, fileName);
        if (file.exists()) {
            RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
            FileChannel channel = accessFile.getChannel();
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, Utils.getPageSize());
            int index = (int) (System.currentTimeMillis() % 30);
            String name = fileName.endsWith(".kva") ? "A" : "B";
            System.out.println("Damage " + name + " file's byte at index:" + index);
            buffer.put(index, (byte) (~buffer.get(index)));
        }
    }

    @Test
    public void testDamageFile() throws IOException, InterruptedException {
        String name = "test_damage_file";
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name).disableWatchFileChange().build();
        kv1.clear();
        kv1.putString("flag", "hello");
        String str = TestHelper.makeString(20);
        for (int i = 0; i < 10; i++) {
            kv1.putString("str_" + i, str);
        }
        kv1.commit();
        Thread.sleep(50L);
        MPFastKV kvt1 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals("hello", kvt1.getString("flag"));

        int e1 = TestHelper.fileErrorCount.get();

        damageFastKVFile(name + ".kva");

        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals("hello", kv2.getString("flag"));
        int e2 = TestHelper.fileErrorCount.get();
        Assert.assertEquals(1, e2 - e1);

        damageFastKVFile(name + ".kvb");

        MPFastKV kv3 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals("hello", kv3.getString("flag"));
        int e3 = TestHelper.fileErrorCount.get();
        Assert.assertEquals(1, e3 - e2);


        MPFastKV kv4 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals("hello", kv4.getString("flag"));
        int e4 = TestHelper.fileErrorCount.get();
        Assert.assertEquals(0, e4 - e3);

        damageFastKVFile(name + ".kva");
        damageFastKVFile(name + ".kvb");

        MPFastKV kv5 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals(0, kv5.getAll().size());

        kv5.putString("key1", "a");
        kv5.putString("key2", "b");
        kv5.putString("key1", "A");
        kv5.commit();
        Thread.sleep(50L);
        MPFastKV kv6 = new MPFastKV(TestHelper.MP_DIR, name, null, null, false);
        Assert.assertEquals("A", kv6.getString("key1"));
    }

    @Test
    public void testEncrypt() {
        testEncryptExternal();
        testPlainToCipher();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void clearFile(String name) {
        new File(TestHelper.MP_DIR, name + ".kva").delete();
        new File(TestHelper.MP_DIR, name + ".kvb").delete();
        new File(TestHelper.MP_DIR, name + ".kvc").delete();
        new File(TestHelper.MP_DIR, name + ".tmp").delete();
        new File(TestHelper.MP_DIR, name).delete();
    }

    private void testEncryptExternal() {
        String name = "encrypt_external";

        clearFile(name);

        FastEncoder<?>[] encoders = new FastEncoder[]{TestObjectEncoder.INSTANCE};

        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name)
                .cipher(TestHelper.cipher)
                .encoder(encoders)
                .build();

        String longStr = TestHelper.makeString(10000);

        byte[] longArray = new byte[10000];
        longArray[100] = 100;

        TestObject obj = new TestObject(12345, longStr);
        kv1.putObject("obj", obj, TestObjectEncoder.INSTANCE);

        Assert.assertEquals(obj, kv1.getObject("obj"));

        kv1.putString("string", longStr);
        kv1.putString("a", "a");
        kv1.putInt("int", 100);
        kv1.putArray("array", longArray);
        kv1.commit();

        try {
            Thread.sleep(100L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, encoders, TestHelper.cipher, false);
        Assert.assertEquals(100, kv2.getInt("int"));
        Assert.assertEquals(longStr, kv2.getString("string"));
        Assert.assertArrayEquals(longArray, kv2.getArray("array"));
        Assert.assertEquals(obj, kv2.getObject("obj"));
    }

    private void testPlainToCipher() {
        String name = "plain_to_cipher";

        clearFile(name);

        FastEncoder<?>[] encoders = new FastEncoder[]{TestObjectEncoder.INSTANCE};

        // not encrypted
        MPFastKV kv1 = new MPFastKV.Builder(TestHelper.MP_DIR, name)
                .encoder(encoders)
                .build();

        String longStr = TestHelper.makeString(10000);

        byte[] longArray = new byte[10000];
        longArray[100] = 100;

        TestObject obj = new TestObject(12345, longStr);
        kv1.putObject("obj", obj, TestObjectEncoder.INSTANCE);
        Assert.assertEquals(obj, kv1.getObject("obj"));

        double d1 = 3.14;

        kv1.putString("string", longStr);
        kv1.putString("s1", "hello");
        kv1.putInt("int", 100);
        kv1.putArray("array", longArray);
        kv1.putDouble("double", d1);
        kv1.commit();

        try {
            // Waiting kv1 to finish saving big value.
            Thread.sleep(100L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // test encrypt
        MPFastKV kv2 = new MPFastKV(TestHelper.MP_DIR, name, encoders, TestHelper.cipher, false);
        Assert.assertEquals(d1, kv1.getDouble("double"), 0);
        Assert.assertEquals("hello", kv2.getString("s1"));
        Assert.assertEquals(100, kv2.getInt("int"));
        Assert.assertEquals(longStr, kv2.getString("string"));
        Assert.assertArrayEquals(longArray, kv2.getArray("array"));
        Assert.assertEquals(obj, kv2.getObject("obj"));
    }
}
