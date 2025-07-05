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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import io.fastkv.fastkvdemo.base.AppContext;
import io.fastkv.interfaces.FastEncoder;

/*
 * 通常我们必须通过 FastKV.Builder 创建 FastKV 实例，
 * 但为了重现"重新打开和加载"的情况，我们将测试放在与 FastKV 相同的包路径中，
 * 这样我们就可以通过构造函数创建 FastKV 实例，在一个执行过程中测试加载。
 * 而后续的打开文件只是进行读取（在两个实例中写入可能会损坏数据）。
 * 注意：正常情况下使用 FastKV.Builder 创建实例。
 */
@SuppressWarnings("SimplifiableJUnitAssertion")
public class FastKVTest {
    @Before
    public void init() {
        FastKVConfig.setLogger(TestHelper.logger);
    }

    @Test
    public void testPutAndGet() {
        FastEncoder<?>[] encoders = new FastEncoder[]{LongListEncoder.INSTANCE};

        String name = "test_put_and_get";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).encoder(encoders).build();
        kv1.clear();

        String boolKey = "bool_key";
        kv1.putBoolean(boolKey, true);

        String intKey = "int_key";
        kv1.putInt(intKey, 1234);

        String floatKey = "float_key";
        kv1.putFloat(floatKey, 3.14f);

        String longKey = "long_key";
        kv1.putLong(longKey, Long.MAX_VALUE);

        String doubleKey = "double_key";
        kv1.putDouble(doubleKey, 99.9);

        String stringKey = "string_key";
        kv1.putString(stringKey, "hello, 你好");


        String stringSetKey = "string_set_key";
        Set<String> stringSetValue = TestHelper.makeStringSet();
        kv1.putStringSet(stringSetKey, stringSetValue);

        String stringSetKey2 = "string_set_key2";
        Set<String> stringSetValue2 = new HashSet<>();
        kv1.putStringSet(stringSetKey2, stringSetValue2);

        String objectKey = "object_key";
        List<Long> list = new ArrayList<>();
        list.add(-1L);
        list.add(0L);
        list.add(1L);
        list.add(Long.MAX_VALUE);
        kv1.putObject(objectKey, list, LongListEncoder.INSTANCE);

        FastKV kv2 = new FastKV(TestHelper.DIR, name, encoders, null, FastKV.NON_BLOCKING);
        Assert.assertEquals(kv1.getBoolean(boolKey), kv2.getBoolean(boolKey));
        Assert.assertEquals(kv1.getInt(intKey), kv2.getInt(intKey));
        Assert.assertTrue(kv1.getFloat(floatKey) == kv2.getFloat(floatKey));
        Assert.assertEquals(kv1.getLong(longKey), kv2.getLong(longKey));
        Assert.assertTrue(kv1.getDouble(doubleKey) == kv2.getDouble(doubleKey));
        Assert.assertEquals(kv1.getString(stringKey), kv1.getString(stringKey));
        Assert.assertEquals(stringSetValue, kv2.getStringSet(stringSetKey));
        Assert.assertEquals(stringSetValue2, kv2.getStringSet(stringSetKey2));
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
        FastKV kv3 = new FastKV(TestHelper.DIR, name_2, encoders, null, FastKV.NON_BLOCKING);
        kv3.clear();

        kv3.putAll(all_1, encoderMap);

        FastKV kv4 = new FastKV(TestHelper.DIR, name_2, encoders, null, FastKV.NON_BLOCKING);
        Assert.assertEquals(all_1, kv4.getAll());

        Map<String, Object> m = new HashMap<>();
        m.put("a", "a");
        m.put("b", "b");
        kv3.putAll(m);
        FastKV kv5 = new FastKV(TestHelper.DIR, name_2, encoders, null, FastKV.NON_BLOCKING);
        Assert.assertEquals("a", kv5.getString("a"));
        Assert.assertEquals("b", kv5.getString("b"));

        subTest(kv1, name, encoders);
    }

    private void subTest(FastKV kv1, String name, FastEncoder<?>[] encoders) {
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

        FastKV kv2 = new FastKV(TestHelper.DIR, name, encoders, null, FastKV.NON_BLOCKING);
        Assert.assertEquals(true, kv2.getBoolean("b"));
        Assert.assertEquals(200, kv2.getInt("i"));
        Assert.assertEquals(Long.MIN_VALUE, kv2.getLong("L"));
        Assert.assertTrue(0.9f == kv2.getFloat("f"));
        Assert.assertTrue(1.5D == kv2.getDouble("D"));
        Assert.assertEquals("Hello World", kv2.getString("S"));
        Assert.assertArrayEquals(kv1.getArray(arrayKey), kv2.getArray(arrayKey));
    }

    @Test
    public void testRemove() {
        String name = "test_remove";
        clearFile(name);

        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).blocking().build();
        kv1.putBoolean("b", true);
        kv1.putInt("i", 100);
        kv1.putLong("L", Long.MIN_VALUE);
        kv1.remove("i");

        FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertNotEquals(100, kv2.getInt("i"));
        Assert.assertEquals(Long.MIN_VALUE, kv2.getLong("L"));
    }

    @Test
    public void testGC() {
        String name = "test_gc";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).build();
        kv1.clear();

        // 使用大字符串确保能触发GC（8KB固定阈值，触发条件是16KB无效数据）
        String longStr = TestHelper.makeString(4000);  // 4KB
        String shortStr = TestHelper.makeString(200);

        int gc1 = TestHelper.gcCount.get();
        kv1.putBoolean("bool_1", true);
        kv1.putInt("int_1", 100);
        kv1.putInt("int_2", 200);
        kv1.putString("short_string", shortStr);
        
        // 写入5个4KB字符串 = 20KB，删除后会产生超过16KB的无效数据
        for (int i = 0; i < 5; i++) {
            kv1.putString("large_string_" + i, longStr);
        }
        for (int i = 0; i < 5; i++) {
            kv1.remove("large_string_" + i);
        }
        int gc2 = TestHelper.gcCount.get();
        // 由于8KB固定阈值，应该至少触发1次GC
        Assert.assertTrue("Expected at least 1 GC, but got " + (gc2 - gc1), gc2 - gc1 >= 1);
        
        // 测试键数量阈值
        for (int i = 0; i < 80; i++) {
            kv1.putString("string_" + i, "hello");
        }
        for (int i = 0; i < 80; i++) {
            kv1.remove("string_" + i);
        }
        int gc3 = TestHelper.gcCount.get();
        Assert.assertTrue("Expected at least 1 GC, but got " + (gc3 - gc2), gc3 - gc2 >= 1);

        FastKV kv3 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals(100, kv3.getInt("int_1"));

        // 测试更新操作触发的GC
        for (int i = 0; i < 3; i++) {
            kv1.putString("update_string_" + i, longStr);
        }
        String newLongStr = longStr + "updated";
        for (int i = 0; i < 3; i++) {
            kv1.putString("update_string_" + i, newLongStr);
        }
        int gc4 = TestHelper.gcCount.get();
        // 更新操作可能触发GC，但不强制要求
        
        FastKV kv4 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals(100, kv4.getInt("int_1"));

        kv1.remove("int_2");

        int truncate1 = TestHelper.truncateCount.get();
        for (int i = 0; i < 30; i++) {
            kv1.putString("truncate_string_" + i, longStr);
        }
        for (int i = 0; i < 30; i++) {
            kv1.remove("truncate_string_" + i);
        }
        int truncate2 = TestHelper.truncateCount.get();
        Assert.assertTrue("Expected at least 1 truncate, but got " + (truncate2 - truncate1), truncate2 - truncate1 >= 1);

        FastKV kv5 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals(100, kv5.getInt("int_1"));
        Assert.assertEquals(0, kv5.getInt("int_2"));
        Assert.assertEquals(kv1.getBoolean("bool_1"), kv5.getBoolean("bool_1"));
        Assert.assertEquals("", kv5.getString("empty_str"));
    }

    @Test
    public void testSyncBlockingGC() {
        String name = "test_blocking_gc";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).blocking().build();
        kv1.clear();

        // 使用大字符串确保能触发GC（8KB固定阈值，触发条件是16KB无效数据）
        String longStr = TestHelper.makeString(4000);  // 4KB
        String shortStr = TestHelper.makeString(200);

        int gc1 = TestHelper.gcCount.get();
        kv1.putBoolean("bool_1", true);
        kv1.putInt("int_1", 100);
        kv1.putInt("int_2", 200);
        kv1.putString("short_string", shortStr);
        
        // 写入5个4KB字符串 = 20KB，删除后会产生超过16KB的无效数据
        for (int i = 0; i < 5; i++) {
            kv1.putString("large_string_" + i, longStr);
        }
        for (int i = 0; i < 5; i++) {
            kv1.remove("large_string_" + i);
        }
        int gc2 = TestHelper.gcCount.get();
        // 由于8KB固定阈值，应该至少触发1次GC
        Assert.assertTrue("Expected at least 1 GC, but got " + (gc2 - gc1), gc2 - gc1 >= 1);
        
        // 测试键数量阈值
        for (int i = 0; i < 80; i++) {
            kv1.putString("string_" + i, "hello");
        }
        for (int i = 0; i < 80; i++) {
            kv1.remove("string_" + i);
        }
        int gc3 = TestHelper.gcCount.get();
        Assert.assertTrue("Expected at least 1 GC, but got " + (gc3 - gc2), gc3 - gc2 >= 1);

        FastKV kvt3 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertEquals(100, kvt3.getInt("int_1"));

        // 测试更新操作触发的GC
        for (int i = 0; i < 3; i++) {
            kv1.putString("update_string_" + i, longStr);
        }
        String newLongStr = longStr + "updated";
        for (int i = 0; i < 3; i++) {
            kv1.putString("update_string_" + i, newLongStr);
        }
        int gc4 = TestHelper.gcCount.get();
        // 更新操作可能触发GC，但不强制要求

        FastKV kvt4 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertEquals(100, kvt4.getInt("int_1"));

        kv1.remove("int_2");

        int truncate1 = TestHelper.truncateCount.get();
        for (int i = 0; i < 30; i++) {
            kv1.putString("truncate_string_" + i, longStr);
        }
        for (int i = 0; i < 30; i++) {
            kv1.remove("truncate_string_" + i);
        }
        int truncate2 = TestHelper.truncateCount.get();
        Assert.assertTrue("Expected at least 1 truncate, but got " + (truncate2 - truncate1), truncate2 - truncate1 >= 1);

        FastKV kv3 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertEquals(100, kv3.getInt("int_1"));
        Assert.assertEquals(0, kv3.getInt("int_2"));
        Assert.assertEquals(kv1.getBoolean("bool_1"), kv3.getBoolean("bool_1"));
        Assert.assertEquals("", kv3.getString("empty_str"));
    }


    @Test
    public void testBigString() {
        String name = "testBigString";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).build();
        kv1.clear();
        String longStr = TestHelper.makeString(10000);
        kv1.putString("str", longStr);
        kv1.putString("a", "a");
        kv1.putInt("int", 100);
        Assert.assertEquals(longStr, kv1.getString("str"));

        // 通过 Builder 创建 FastKV 实例，如果名字相同，会获取到相同的实例，
        // 要想同一个测试用例函数中测试“加载”，可以现关闭当前实例(会移除实例缓存），
        // 再重新用 Builder 创建，就可以重新创建并加载了。
        kv1.close();

        FastKV kv2 = new FastKV.Builder(TestHelper.DIR, name).build();
        Assert.assertEquals(longStr, kv2.getString("str"));
        Assert.assertEquals(100, kv2.getInt("int"));

        kv2.putString("str", "hello");
        Assert.assertEquals("hello", kv2.getString("str"));

        longStr += "1";
        kv2.putString("str", longStr);
        kv2.putString("str2", longStr);
        Assert.assertEquals(longStr, kv2.getString("str"));
        Assert.assertEquals(longStr, kv2.getString("str2"));

        // 测试连续写入不同的大value
        longStr += "a";
        kv2.putString("str", longStr);
        longStr += "a";
        kv2.putString("str", longStr);
        Assert.assertEquals(longStr, kv2.getString("str"));

        kv2.close();

        FastKV kv3 = new FastKV.Builder(TestHelper.DIR, name).build();
        Assert.assertEquals(longStr, kv3.getString("str"));
    }

    @Test
    public void testBigArray() {
        String name = "testBigArray";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).build();
        kv1.clear();
        byte[] longArray = new byte[10000];
        kv1.putArray("array", longArray);
        kv1.putString("a", "a");
        kv1.putInt("int", 100);

        byte[] shortArray = "hello".getBytes(StandardCharsets.UTF_8);
        kv1.putArray("array", shortArray);
        Assert.assertArrayEquals(shortArray, kv1.getArray("array"));

        kv1.putArray("array", longArray);
        Assert.assertArrayEquals(longArray, kv1.getArray("array"));

        kv1.close();

        FastKV kv2 = new FastKV.Builder(TestHelper.DIR, name).build();
        Assert.assertArrayEquals(longArray, kv2.getArray("array"));
        Assert.assertEquals(100, kv2.getInt("int"));
    }

    @Test
    public void testBigObject() {
        String name = "testBigObject";
        FastEncoder<?>[] encoders = new FastEncoder[]{TestObjectEncoder.INSTANCE};
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).encoder(encoders).build();
        kv1.clear();
        String longStr = TestHelper.makeString(10000);
        TestObject obj = new TestObject(12345, longStr);
        kv1.putObject("obj", obj.copy(), TestObjectEncoder.INSTANCE);
        kv1.putString("a", "a");
        kv1.putInt("int", 100);

        Assert.assertEquals(obj, kv1.getObject("obj"));
        Assert.assertEquals(100, kv1.getInt("int"));

        obj.id = 123456;
        obj.info = "hello";
        kv1.putObject("obj", obj.copy(), TestObjectEncoder.INSTANCE);
        Assert.assertEquals(obj, kv1.getObject("obj"));

        obj.id = 123457;
        obj.info = longStr;
        kv1.putObject("obj", obj.copy(), TestObjectEncoder.INSTANCE);
        Assert.assertEquals(obj, kv1.getObject("obj"));
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
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).build();
        kv1.clear();

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
            long t2 = System.nanoTime();
            int gc2 = TestHelper.gcCount.get();
            System.out.println("use time:" + ((t2 - t1) / 1000000) + ", gc times:" + (gc2 - gc1));
            FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
            Assert.assertEquals("flag1", kv2.getString(flag1));
            Assert.assertEquals(100, kv2.getInt(flag2));
        }
    }

    @Test
    public void testNotASCII() {
        String name = "test_not_ascii";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).build();
        kv1.clear();
        String[] notAscii = new String[]{"\uD842\uDFB7", "一", "二", "三", "四", "五"};
        int i = 1000;
        for (String str : notAscii) {
            kv1.putString(str, str + i);
            i++;
        }
        FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
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
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).build();
        kv1.clear();

        long seed = System.nanoTime();
        System.out.println("random test seed: " + seed);
        Random r = new Random(seed);

        ArrayList<Pair<String, Object>> srcList = generateInputList(loadSourceData());

        long time = 0;
        List<Pair<String, Object>> inputList = new ArrayList<>(srcList);
        for (int i = 0; i < 1; i++) {
            long t1 = System.nanoTime();
            putToFastKV(kv1, inputList);
            long t2 = System.nanoTime();
            time += (t2 - t1);

        }
        System.out.println("fill, use time: " + (time / 1000000) + " ms");

        kv1.putString("flag", "hello");

        int round = 3;
        time = 0;
        for (int i = 0; i < round; i++) {
            inputList = getDistributedList(srcList, r, 3);
            long t1 = System.nanoTime();
            putToFastKV(kv1, inputList);
            long t2 = System.nanoTime();
            time += (t2 - t1);
        }
        System.out.println("update, use time: " + (time / 1000000) + " ms");

        FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals("hello", kv2.getString("flag"));
    }

    private static void putToFastKV(FastKV fastkv, List<Pair<String, Object>> list) {
        for (Pair<String, Object> pair : list) {
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
    }

    static class Pair<F, S> {
        public final F first;
        public final S second;

        public Pair(F first, S second) {
            this.first = first;
            this.second = second;
        }
    }

    private static ArrayList<Pair<String, Object>> generateInputList(Map<String, ?> all) {
        ArrayList<Pair<String, Object>> list = new ArrayList<>(all.size());
        for (Map.Entry<String, ?> entry : all.entrySet()) {
            list.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        return list;
    }

    private Map<String, ?> loadSourceData() throws IOException {
        String srcName = "src";
        File srcFile = new File(TestHelper.DIR, "src.kva");
        if (!srcFile.exists()) {

            InputStream inputStream = AppContext.INSTANCE.getContext().getAssets().open("src.kva");
            //this.getClass().getClassLoader().getResourceAsStream("src.kva");
            if (inputStream == null) {
                throw new IOException("Could not load src.kva");
            }
            byte[] bytes = IOUtil.streamToBytes(inputStream);
            IOUtil.bytesToFile(bytes, srcFile);
        }
        return new FastKV.Builder(TestHelper.DIR, srcName).build().getAll();
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

    private static List<Pair<String, Object>> getDistributedList(List<Pair<String, Object>> srcList, Random r, int n) {
        List<Pair<String, Object>> inputList = new ArrayList<>(srcList.size());
        int[] a = getDistributedArray(srcList.size(), n, r);
        for (int index : a) {
            Pair<String, Object> pair = srcList.get(index);
            inputList.add(new Pair<>(pair.first, tuningObject(pair.second, r)));
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
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).build();
        long newTime = System.currentTimeMillis() ^ System.nanoTime();

        kv1.putLong("time", newTime);
        kv1.force();

        File aFile = new File(TestHelper.DIR, name + ".kva");
        RandomAccessFile accessFile = new RandomAccessFile(aFile, "r");
        ByteBuffer buffer = ByteBuffer.allocate(26);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        accessFile.read(buffer.array(), 0, 26);
        long t = buffer.getLong(18);
        Assert.assertEquals(newTime, t);
    }

    private void damageFastKVFile(String fileName) throws IOException {
        File file = new File(TestHelper.DIR, fileName);
        if (file.exists()) {
            RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
            FileChannel channel = accessFile.getChannel();
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, FastKV.PAGE_SIZE);
            int index = (int) (System.currentTimeMillis() % 30);
            String name = fileName.endsWith(".kva") ? "A" : "B";
            System.out.println("Damage " + name + " file's byte at index:" + index);
            buffer.put(index, (byte) (~buffer.get(index)));
        }
    }

    @Test
    public void testDamageFile() throws IOException, InterruptedException {
        String name = "test_damage_file";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).build();
        kv1.clear();
        kv1.putString("flag", "hello");
        String str = TestHelper.makeString(20);
        for (int i = 0; i < 10; i++) {
            kv1.putString("str_" + i, str);
        }
        Thread.sleep(50L);
        FastKV kvt1 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals("hello", kvt1.getString("flag"));

        int e1 = TestHelper.fileErrorCount.get();

        damageFastKVFile(name + ".kva");

        FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals("hello", kv2.getString("flag"));
        int e2 = TestHelper.fileErrorCount.get();
        Assert.assertEquals(1, e2 - e1);

        damageFastKVFile(name + ".kvb");

        FastKV kv3 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals("hello", kv3.getString("flag"));
        int e3 = TestHelper.fileErrorCount.get();
        Assert.assertEquals(1, e3 - e2);


        FastKV kv4 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals("hello", kv4.getString("flag"));
        int e4 = TestHelper.fileErrorCount.get();
        Assert.assertEquals(0, e4 - e3);

        damageFastKVFile(name + ".kva");
        damageFastKVFile(name + ".kvb");

        FastKV kv5 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals(0, kv5.getAll().size());

        kv5.putString("key1", "a");
        kv5.putString("key2", "b");
        kv5.putString("key1", "A");
        Thread.sleep(50L);
        FastKV kv6 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals("A", kv6.getString("key1"));
    }

    @Test
    public void testSync() throws Exception {
        String name = "test_sync";
        File cFile = new File(TestHelper.DIR, name + ".kvc");
        if (cFile.exists()) {
            cFile.delete();
        }

        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).blocking().build();
        // kv1.clear();

        Assert.assertEquals(false, kv1.contains("time"));

        long newTime = System.currentTimeMillis() ^ System.nanoTime();
        kv1.putLong("time", newTime);

        File aFile = new File(TestHelper.DIR, name + ".kvc");
        RandomAccessFile accessFile = new RandomAccessFile(aFile, "r");
        ByteBuffer buffer = ByteBuffer.allocate(26);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        accessFile.read(buffer.array(), 0, 26);
        long t = buffer.getLong(18);
        Assert.assertEquals(newTime, t);

        FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertEquals(newTime, kv2.getLong("time"));

        kv1.putLong("time", 100L);
        FastKV kv3 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertEquals(100L, kv3.getLong("time"));
    }

    @Test
    public void testSync2() throws IOException {
        String name = "test_sync2";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).blocking().build();
        kv1.clear();

        long seed = System.nanoTime();
        System.out.println("random test seed: " + seed);
        Random r = new Random(seed);

        ArrayList<Pair<String, Object>> srcList = generateInputList(loadSourceData());

        long time = 0;
        List<Pair<String, Object>> inputList = new ArrayList<>(srcList);
        for (int i = 0; i < 1; i++) {
            long t1 = System.nanoTime();
            putToFastKV(kv1, inputList);
            long t2 = System.nanoTime();
            time += (t2 - t1);

        }
        System.out.println("fill, use time: " + (time / 1000000) + " ms");

        kv1.putString("flag", "hello");

        int round = 3;
        time = 0;
        for (int i = 0; i < round; i++) {
            inputList = getDistributedList(srcList, r, 3);
            long t1 = System.nanoTime();
            putToFastKV(kv1, inputList);
            long t2 = System.nanoTime();
            time += (t2 - t1);
        }
        System.out.println("update, use time: " + (time / 1000000) + " ms");

        FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertEquals("hello", kv2.getString("flag"));
    }

    @Test
    public void testAsync() throws Exception {
        String name = "test_async";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).asyncBlocking().build();
        kv1.clear();

        Assert.assertEquals(false, kv1.contains("time"));

        long newTime = System.currentTimeMillis() ^ System.nanoTime();
        kv1.putLong("time", newTime);
        Thread.sleep(50L);

        File aFile = new File(TestHelper.DIR, name + ".kvc");
        RandomAccessFile accessFile = new RandomAccessFile(aFile, "r");
        ByteBuffer buffer = ByteBuffer.allocate(26);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        accessFile.read(buffer.array(), 0, 26);
        long t = buffer.getLong(18);
        Assert.assertEquals(newTime, t);

        FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.ASYNC_BLOCKING);
        Assert.assertEquals(newTime, kv2.getLong("time"));

        kv1.putLong("time", 100L);
        Thread.sleep(50L);
        FastKV kv3 = new FastKV(TestHelper.DIR, name, null, null, FastKV.ASYNC_BLOCKING);
        Assert.assertEquals(100L, kv3.getLong("time"));
    }

    @Test
    public void testDisableAutoCommit() throws Exception {
        String name = "test_disable_auto_commit";
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).blocking().build();
        kv1.clear();

        Assert.assertEquals(false, kv1.contains("time"));
        long newTime = System.currentTimeMillis() ^ System.nanoTime();

        kv1.disableAutoCommit();
        kv1.putLong("time", newTime);
        kv1.putString("str", "hello");
        kv1.putInt("int", 100);

        FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertNotEquals(100, kv2.getInt("int"));

        boolean result = kv1.commit();
        Assert.assertEquals(true, result);

        FastKV kv3 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertEquals(100, kv3.getInt("int"));

        File aFile = new File(TestHelper.DIR, name + ".kvc");
        RandomAccessFile accessFile = new RandomAccessFile(aFile, "r");
        ByteBuffer buffer = ByteBuffer.allocate(26);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        accessFile.read(buffer.array(), 0, 26);
        long t = buffer.getLong(18);
        Assert.assertEquals(newTime, t);

        kv1.putBoolean("bool", false);
        kv1.putBoolean("bool", true);

        FastKV kv4 = new FastKV(TestHelper.DIR, name, null, null, FastKV.SYNC_BLOCKING);
        Assert.assertEquals(newTime, kv4.getLong("time"));
        Assert.assertEquals(true, kv4.getBoolean("bool"));
    }

    @Test
    public void testEncrypt() {
        testEncryptExternal();
        testPlainToCipher();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void clearFile(String name) {
        new File(TestHelper.DIR, name + ".kva").delete();
        new File(TestHelper.DIR, name + ".kvb").delete();
        new File(TestHelper.DIR, name + ".kvc").delete();
        new File(TestHelper.DIR, name + ".tmp").delete();
        new File(TestHelper.DIR, name).delete();
    }

    private void testEncryptExternal() {
        String name = "encrypt_external";

        clearFile(name);

        FastEncoder<?>[] encoders = new FastEncoder[]{TestObjectEncoder.INSTANCE};

        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name)
                .cipher(TestHelper.cipher)
                .encoder(encoders)
                .build();

        String longStr = TestHelper.makeString(10000);

        byte[] longArray = new byte[10000];
        longArray[100] = 100;

        TestObject obj = new TestObject(12345, longStr);
        kv1.putObject("obj", obj.copy(), TestObjectEncoder.INSTANCE);

        Assert.assertEquals(obj, kv1.getObject("obj"));

        kv1.putString("string", longStr);
        kv1.putString("a", "a");
        kv1.putInt("int", 100);
        kv1.putArray("array", longArray);

        kv1.close();

        FastKV kv2 = new FastKV.Builder(TestHelper.DIR, name)
                .cipher(TestHelper.cipher)
                .encoder(encoders)
                .build();
        Assert.assertEquals(100, kv2.getInt("int"));
        Assert.assertEquals(longStr, kv2.getString("string"));
        Assert.assertArrayEquals(longArray, kv2.getArray("array"));
        Assert.assertEquals(obj, kv2.getObject("obj"));
    }

    private void testPlainToCipher() {
        String name = "plain_to_cipher";

        clearFile(name);

        FastEncoder<?>[] encoders = new FastEncoder[]{TestObjectEncoder.INSTANCE};

        // 未加密
        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name)
                .encoder(encoders)
                .build();

        String longStr = TestHelper.makeString(10000);

        byte[] longArray = new byte[10000];
        longArray[100] = 100;

        TestObject obj = new TestObject(12345, longStr);
        kv1.putObject("obj", obj.copy(), TestObjectEncoder.INSTANCE);
        Assert.assertEquals(obj, kv1.getObject("obj"));

        double d1 = 3.14;

        kv1.putString("string", longStr);
        kv1.putString("s1", "hello");
        kv1.putInt("int", 100);
        kv1.putArray("array", longArray);
        kv1.putDouble("double", d1);

        kv1.close();

        // 加密
        FastKV kv2 = new FastKV.Builder(TestHelper.DIR, name)
                .cipher(TestHelper.cipher)
                .encoder(encoders)
                .build();
        Assert.assertEquals(d1, kv1.getDouble("double"), 0);
        Assert.assertEquals("hello", kv2.getString("s1"));
        Assert.assertEquals(100, kv2.getInt("int"));
        Assert.assertEquals(longStr, kv2.getString("string"));
        Assert.assertArrayEquals(longArray, kv2.getArray("array"));
        Assert.assertEquals(obj, kv2.getObject("obj"));
    }

    /**
     * 保存一个key-value, 调用其他类型的get方法：
     * 验证不会崩溃，并且支持类型转换。
     */
    @Test
    public void testGetDifferentType() {
        String name = "test_get_different_type";
        FastKV kv = new FastKV.Builder(TestHelper.DIR, name).build();
        kv.clear();
        String key = "test";
        kv.putBoolean(key, true);

        // 现在支持类型转换
        Assert.assertEquals(true, kv.getBoolean(key));
        Assert.assertEquals(1, kv.getInt(key));        // boolean true -> int 1
        Assert.assertTrue(1f == kv.getFloat(key));     // boolean true -> float 1.0f
        Assert.assertEquals(1L, kv.getLong(key));      // boolean true -> long 1L
        Assert.assertTrue(1D == kv.getDouble(key));    // boolean true -> double 1.0
        Assert.assertEquals("true", kv.getString(key)); // boolean true -> string "true"
        Assert.assertArrayEquals(new byte[0], kv.getArray(key)); // Array类型使用默认实现
        Assert.assertTrue(null == kv.getObject(key));   // Object类型使用默认实现
    }

    /**
     * 往同一个key put不同的value：
     * 验证不会崩溃，并且保存最后一次put的value。
     */
    @Test
    public void testPutDifferentType() {
        String name = "test_put_different_type";
        FastKV kv = new FastKV.Builder(TestHelper.DIR, name).build();
        kv.clear();
        String key = "test";
        kv.putBoolean(key, true);
        Assert.assertEquals(true, kv.getBoolean(key));

        kv.putInt(key, 1);
        Assert.assertEquals(1, kv.getInt(key));

        kv.putFloat(key, 1.5f);
        Assert.assertTrue(1.5f == kv.getFloat(key));

        kv.putLong(key, 2);
        Assert.assertEquals(2, kv.getLong(key));

        kv.putDouble(key, 2.5D);
        Assert.assertTrue(2.5D == kv.getDouble(key));

        kv.putString(key, "test");
        Assert.assertEquals("test", kv.getString(key));

        byte[] testArray = new byte[]{1, 2};
        kv.putArray(key, testArray);
        Assert.assertArrayEquals(testArray, kv.getArray(key));

        Set<String> testSet = new HashSet<>();
        testSet.add("1");
        testSet.add("2");
        kv.putStringSet(key, testSet);
        Assert.assertEquals(testSet, kv.getStringSet(key));

        FastKV kv1 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertEquals(testSet, kv1.getStringSet(key));
    }

    @Test
    public void testLargeTypes() {
        String name = "test_large_types";
        clearFile(name);

        FastKV kv1 = new FastKV.Builder(TestHelper.DIR, name).build();
        
        // 测试大字符串（超过64KB）
        StringBuilder largeStringBuilder = new StringBuilder();
        for (int i = 0; i < 70000; i++) {
            largeStringBuilder.append("a");
        }
        String largeString = largeStringBuilder.toString();
        kv1.putString("large_string", largeString);
        
        // 测试大数组（超过64KB）
        byte[] largeArray = new byte[70000];
        for (int i = 0; i < largeArray.length; i++) {
            largeArray[i] = (byte) (i % 256);
        }
        kv1.putArray("large_array", largeArray);
        
        // 测试小字符串和小数组（确保向后兼容）
        String smallString = "small";
        byte[] smallArray = new byte[100];
        for (int i = 0; i < smallArray.length; i++) {
            smallArray[i] = (byte) i;
        }
        kv1.putString("small_string", smallString);
        kv1.putArray("small_array", smallArray);
        
        // 重新打开文件进行验证
        FastKV kv2 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        
        // 验证大字符串
        String retrievedLargeString = kv2.getString("large_string");
        Assert.assertEquals(largeString.length(), retrievedLargeString.length());
        Assert.assertEquals(largeString, retrievedLargeString);
        
        // 验证大数组
        byte[] retrievedLargeArray = kv2.getArray("large_array");
        Assert.assertArrayEquals(largeArray, retrievedLargeArray);
        
        // 验证小字符串和小数组（确保向后兼容性）
        Assert.assertEquals(smallString, kv2.getString("small_string"));
        Assert.assertArrayEquals(smallArray, kv2.getArray("small_array"));
        
        // 测试边界情况：恰好64KB-1
        byte[] boundaryArray = new byte[65534]; // 64KB - 1
        for (int i = 0; i < boundaryArray.length; i++) {
            boundaryArray[i] = (byte) (i % 256);
        }
        kv1.putArray("boundary_array", boundaryArray);
        
        // 测试边界情况：恰好64KB
        byte[] largeBoundaryArray = new byte[65535]; // 64KB
        for (int i = 0; i < largeBoundaryArray.length; i++) {
            largeBoundaryArray[i] = (byte) (i % 256);
        }
        kv1.putArray("large_boundary_array", largeBoundaryArray);
        
        // 重新打开验证边界情况
        FastKV kv3 = new FastKV(TestHelper.DIR, name, null, null, FastKV.NON_BLOCKING);
        Assert.assertArrayEquals(boundaryArray, kv3.getArray("boundary_array"));
        Assert.assertArrayEquals(largeBoundaryArray, kv3.getArray("large_boundary_array"));
    }
}
