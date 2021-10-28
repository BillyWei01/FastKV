package io.fastkv;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHelper {
    public static final String DIR = "./out/test/";

    static AtomicInteger gcCount = new AtomicInteger();
    static AtomicInteger truncateCount = new AtomicInteger();
    static AtomicInteger fileErrorCount = new AtomicInteger();

    public static FastKV.Logger logger = new FastKV.Logger() {
        @Override
        public void i(String name, String message) {
            if (FastKV.GC_FINISH.equals(message)) {
                gcCount.incrementAndGet();
                //System.out.println("gc count:" + gcCount.get() + ", name: " + name);
            } else {
                if (FastKV.TRUNCATE_FINISH.equals(message)) {
                    truncateCount.incrementAndGet();
                }
                System.out.println("info: " + message + ", name: " + name);
            }
        }

        @Override
        public void w(String name, Exception e) {
            String message = e.getMessage();
            System.out.println("warning: " + e.getMessage() + ", name: " + name);
            if (message != null && message.contains("file error")) {
                fileErrorCount.incrementAndGet();
            } else {
                e.printStackTrace();
            }
        }

        @Override
        public void e(String name, Exception e) {
            System.out.println("error: " + e.getMessage() + ", name: " + name);
            if (!"both files error".equals(e.getMessage())) {
                e.printStackTrace();
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
