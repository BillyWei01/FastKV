package io.fastkv;

import android.content.Context;

import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;

import static org.junit.Assert.*;

@RunWith(AndroidJUnit4.class)
public class TypeCompatibilityTest {

    private Context context;
    private FastKV kv;

    @Before
    public void setUp() throws Exception {
        context = InstrumentationRegistry.getInstrumentation().getTargetContext();
        String path = context.getFilesDir().getAbsolutePath() + "/fastkv_test/";
        // 清理测试目录
        File dir = new File(path);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
        }
        kv = new FastKV.Builder(path, "type_compatibility_test").build();
    }

    @After
    public void tearDown() throws Exception {
        if (kv != null) {
            kv.close();
        }
    }

    @Test
    public void testBooleanConversion() {
        // 测试boolean到其他类型的转换
        kv.putBoolean("bool_true", true);
        kv.putBoolean("bool_false", false);

        // boolean -> int
        assertEquals(1, kv.getInt("bool_true", 0));
        assertEquals(0, kv.getInt("bool_false", 0));

        // boolean -> long
        assertEquals(1L, kv.getLong("bool_true", 0L));
        assertEquals(0L, kv.getLong("bool_false", 0L));

        // boolean -> float
        assertEquals(1.0f, kv.getFloat("bool_true", 0.0f), 0.001f);
        assertEquals(0.0f, kv.getFloat("bool_false", 0.0f), 0.001f);

        // boolean -> double
        assertEquals(1.0, kv.getDouble("bool_true", 0.0), 0.001);
        assertEquals(0.0, kv.getDouble("bool_false", 0.0), 0.001);

        // boolean -> string
        assertEquals("true", kv.getString("bool_true", ""));
        assertEquals("false", kv.getString("bool_false", ""));
    }

    @Test
    public void testIntConversion() {
        // 测试int到其他类型的转换
        kv.putInt("int_positive", 42);
        kv.putInt("int_zero", 0);
        kv.putInt("int_negative", -10);

        // int -> boolean
        assertTrue(kv.getBoolean("int_positive", false));
        assertFalse(kv.getBoolean("int_zero", true));
        assertTrue(kv.getBoolean("int_negative", false));

        // int -> long
        assertEquals(42L, kv.getLong("int_positive", 0L));
        assertEquals(-10L, kv.getLong("int_negative", 0L));

        // int -> float
        assertEquals(42.0f, kv.getFloat("int_positive", 0.0f), 0.001f);
        assertEquals(-10.0f, kv.getFloat("int_negative", 0.0f), 0.001f);

        // int -> double
        assertEquals(42.0, kv.getDouble("int_positive", 0.0), 0.001);
        assertEquals(-10.0, kv.getDouble("int_negative", 0.0), 0.001);

        // int -> string
        assertEquals("42", kv.getString("int_positive", ""));
        assertEquals("-10", kv.getString("int_negative", ""));
    }

    @Test
    public void testFloatConversion() {
        float float_pi = 3.14f;
        // 测试float到其他类型的转换
        kv.putFloat("float_positive", float_pi);
        kv.putFloat("float_zero", 0.0f);

        // float -> boolean
        assertTrue(kv.getBoolean("float_positive", false));
        assertFalse(kv.getBoolean("float_zero", true));

        // float -> int
        assertEquals(3, kv.getInt("float_positive", 0));
        assertEquals(0, kv.getInt("float_zero", 1));

        // float -> long
        assertEquals(3L, kv.getLong("float_positive", 0L));

        // float -> double
        assertEquals(float_pi, kv.getDouble("float_positive", 0.0), 0.001);

        // float -> string
        assertEquals("3.14", kv.getString("float_positive", ""));
    }

    @Test
    public void testStringConversion() {
        // 测试string到其他类型的转换
        kv.putString("str_true", "true");
        kv.putString("str_false", "false");
        kv.putString("str_int", "123");
        kv.putString("str_float", "45.67");
        kv.putString("str_zero", "0");
        kv.putString("str_empty", "");

        // string -> boolean
        assertTrue(kv.getBoolean("str_true", false));
        assertFalse(kv.getBoolean("str_false", true));
        assertFalse(kv.getBoolean("str_int", true));  // "123" 不是 "true"，返回false
        assertFalse(kv.getBoolean("str_zero", true)); // "0" 不是 "true"，返回false
        assertFalse(kv.getBoolean("str_empty", true)); // 空字符串不是 "true"，返回false

        // string -> int
        assertEquals(123, kv.getInt("str_int", 0));
        assertEquals(45, kv.getInt("str_float", 0));
        assertEquals(0, kv.getInt("str_zero", 1));
        assertEquals(0, kv.getInt("str_empty", 1));

        // string -> float
        assertEquals(123.0f, kv.getFloat("str_int", 0.0f), 0.001f);
        assertEquals(45.67f, kv.getFloat("str_float", 0.0f), 0.001f);

        // string -> double
        assertEquals(123.0, kv.getDouble("str_int", 0.0), 0.001);
        assertEquals(45.67, kv.getDouble("str_float", 0.0), 0.001);
    }

    @Test
    public void testLongConversion() {
        // 测试long到其他类型的转换
        kv.putLong("long_large", 1234567890123L);
        kv.putLong("long_zero", 0L);

        // long -> boolean
        assertTrue(kv.getBoolean("long_large", false));
        assertFalse(kv.getBoolean("long_zero", true));

        // long -> int (会溢出，不测试具体值)
        assertTrue(kv.getInt("long_large", 0) != 0); // 只验证不是默认值

        // long -> float
        assertEquals(1.2345679E12f, kv.getFloat("long_large", 0.0f), 1E8f);

        // long -> double
        assertEquals(1.234567890123E12, kv.getDouble("long_large", 0.0), 1E6);

        // long -> string
        assertEquals("1234567890123", kv.getString("long_large", ""));
    }

    @Test
    public void testDoubleConversion() {
        // 测试double到其他类型的转换
        kv.putDouble("double_pi", 3.141592653589793);
        kv.putDouble("double_zero", 0.0);

        // double -> boolean
        assertTrue(kv.getBoolean("double_pi", false));
        assertFalse(kv.getBoolean("double_zero", true));

        // double -> int
        assertEquals(3, kv.getInt("double_pi", 0));

        // double -> long
        assertEquals(3L, kv.getLong("double_pi", 0L));

        // double -> float
        assertEquals(3.1415927f, kv.getFloat("double_pi", 0.0f), 0.001f);

        // double -> string
        assertEquals("3.141592653589793", kv.getString("double_pi", ""));
    }

    @Test
    public void testArrayConversion() {
        // 测试byte[]到其他类型的转换（使用默认值）
        byte[] data = {1, 2, 3, 4, 5};
        byte[] empty = {};
        kv.putArray("array_data", data);
        kv.putArray("array_empty", empty);

        // array -> boolean (ArrayContainer使用默认实现，返回false)
        assertFalse(kv.getBoolean("array_data", true));
        assertFalse(kv.getBoolean("array_empty", true));

        // array -> int/long/float/double (ArrayContainer使用默认实现，返回0)
        assertEquals(0, kv.getInt("array_data", 1));
        assertEquals(0L, kv.getLong("array_data", 1L));
        assertEquals(0.0f, kv.getFloat("array_data", 1.0f), 0.001f);
        assertEquals(0.0, kv.getDouble("array_data", 1.0), 0.001);

        // array -> string (ArrayContainer使用默认实现，返回空字符串)
        assertEquals("", kv.getString("array_data", "default"));
        assertEquals("", kv.getString("array_empty", "default"));
    }

    @Test
    public void testCompatibilityWithOriginalTypes() {
        // 确保原本的类型匹配仍然正常工作
        kv.putBoolean("bool", true);
        kv.putInt("int", 42);
        kv.putFloat("float", 3.14f);
        kv.putLong("long", 1234L);
        kv.putDouble("double", 2.718);
        kv.putString("string", "hello");

        // 原类型应该正常返回
        assertTrue(kv.getBoolean("bool", false));
        assertEquals(42, kv.getInt("int", 0));
        assertEquals(3.14f, kv.getFloat("float", 0.0f), 0.001f);
        assertEquals(1234L, kv.getLong("long", 0L));
        assertEquals(2.718, kv.getDouble("double", 0.0), 0.001);
        assertEquals("hello", kv.getString("string", ""));
    }

    @Test
    public void testSpecialStringValues() {
        // 测试特殊字符串值的转换
        kv.putString("yes", "yes");
        kv.putString("no", "no");
        kv.putString("one", "1");
        kv.putString("negative", "-42");

        // 特殊字符串 -> boolean (简化后只识别"true"和"false")
        assertFalse(kv.getBoolean("yes", true));    // "yes" 不是 "true"，返回false
        assertFalse(kv.getBoolean("no", true));     // "no" 不是 "true"，返回false
        assertFalse(kv.getBoolean("one", true));    // "1" 不是 "true"，返回false
        assertFalse(kv.getBoolean("negative", true)); // "-42" 不是 "true"，返回false

        // 负数字符串 -> int
        assertEquals(-42, kv.getInt("negative", 0));
    }
} 