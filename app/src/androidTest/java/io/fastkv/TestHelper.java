package io.fastkv;

import android.util.Log;

import androidx.annotation.NonNull;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.fastkv.fastkvdemo.fastkv.cipher.AESCipher;
import io.fastkv.fastkvdemo.manager.PathManager;
import io.fastkv.interfaces.FastCipher;
import io.fastkv.interfaces.FastLogger;

public class TestHelper {
    private static final String TAG = "TestHelper";
    public static final String DIR = PathManager.INSTANCE.getFilesDir() + "/test/";
    public static final String MP_DIR = PathManager.INSTANCE.getFilesDir() + "/mp_test/";

    static final FastCipher cipher = new AESCipher("1234567890abcdef".getBytes());

    static AtomicInteger gcCount = new AtomicInteger();
    static AtomicInteger truncateCount = new AtomicInteger();
    static AtomicInteger fileErrorCount = new AtomicInteger();

    public static FastLogger logger = new FastLogger() {
        @Override
        public void i(@NonNull String name, @NonNull String message) {
            if (FastKV.GC_FINISH.equals(message)) {
                gcCount.incrementAndGet();
                //System.out.println("gc count:" + gcCount.get() + ", name: " + name);
            } else {
                if (FastKV.TRUNCATE_FINISH.equals(message)) {
                    truncateCount.incrementAndGet();
                }
                Log.i(TAG, "info: " + message + ", name: " + name);
            }
        }

        @Override
        public void w(@NonNull String name, @NonNull Exception e) {
            String message = e.getMessage();
            Log.w(TAG,"warning: " + e.getMessage() + ", name: " + name);
            if (message != null && message.contains("file error")) {
                fileErrorCount.incrementAndGet();
            } else {
                Log.w(TAG, e);
            }
        }

        @Override
        public void e(@NonNull String name, @NonNull Exception e) {
            Log.e(TAG, "error: " + e.getMessage() + ", name: " + name);
            if (!"both files error".equals(e.getMessage())) {
                Log.e(TAG, e.getMessage(), e);
            }
        }
    };

    public static Set<String> makeStringSet() {
        Set<String> set = new LinkedHashSet<>();
        set.add("spring");
        set.add("summer");
        set.add("autumn");
        set.add("winter");
        set.add(null);
        set.add("");
        set.add("end");
        return set;
    }

    public static String makeString(int size) {
        char[] a = new char[size];
        for (int i = 0; i < size; i++) {
            a[i] = 'a';
        }
        return new String(a);
    }
}
