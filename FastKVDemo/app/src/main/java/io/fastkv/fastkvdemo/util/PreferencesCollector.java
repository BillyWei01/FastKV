package io.fastkv.fastkvdemo.util;

import android.content.Context;
import android.content.SharedPreferences;
import android.util.Log;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class PreferencesCollector {
    private static final String TAG = "Collector";
    private static final char[] NOT_ASCII = new char[]{'一', '二', '三', '四', '五', '六'};
    private static final char[] DIGITS = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
            'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
            'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_',
    };
    private static final Random r = new Random(1);
    private static final Set<Character> specialCharSet = new HashSet<>() ;
    private static final int LENGTH_LIMIT  = 1000;

    static {
        char[] a = new char[]{'<', '>','/','"','\'','='};
        for (char ch : a) {
            specialCharSet.add(ch);
        }
    }

    private static String transferString(String src) {
        char[] a = src.toCharArray();
        int n = a.length;
        for (int i = 0; i < n; i++) {
            if(!specialCharSet.contains(a[i])){
                if (a[i] < 0x80) {
                    a[i] = DIGITS[Math.abs(r.nextInt()) % DIGITS.length];
                } else {
                    a[i] = NOT_ASCII[Math.abs(r.nextInt()) % NOT_ASCII.length];
                }
            }
        }
        return new String(a);
    }

    public static void collectPreference(Context context, Set<String> exclude) {
        File spDir = new File(context.getFilesDir().getParent(), "/shared_prefs");
        File[] spFiles = spDir.listFiles();
        if (spFiles == null || spFiles.length == 0) {
            Log.w(TAG, "No SharePreferences files");
            return;
        }
        exclude.add("sum");
        try {
            SharedPreferences sumPreferences = context.getSharedPreferences("sum", Context.MODE_PRIVATE);
            SharedPreferences.Editor editor = sumPreferences.edit();
            editor.clear();
            int booleanCount = 0;
            int intCount = 0;
            int longCount = 0;
            int floatCount = 0;
            int stringCount = 0;
            int stringSetCount = 0;
            for (File spFile : spFiles) {
                String fileName = spFile.getName();
                String name = fileName.substring(0, fileName.lastIndexOf('.'));
                if (exclude.contains(name)) {
                    continue;
                }
                SharedPreferences preferences = context.getSharedPreferences(name, Context.MODE_PRIVATE);
                Map<String, ?> all = preferences.getAll();
                for (Map.Entry<String, ?> entry : all.entrySet()) {
                    final String key = transferString(entry.getKey());
                    final Object obj = entry.getValue();
                    if (obj instanceof String) {
                        String value = (String) obj;
                        if(value.length() > LENGTH_LIMIT){
                            continue;
                        }
                        stringCount++;
                        editor.putString(key, transferString(value));
                    } else if (obj instanceof Boolean) {
                        booleanCount++;
                        editor.putBoolean(key, (Boolean) obj);
                    } else if (obj instanceof Integer) {
                        intCount++;
                        int oldValue = (Integer) obj;
                        editor.putInt(key, (oldValue == 0 ? 0 : r.nextInt() % oldValue));
                    } else if (obj instanceof Long) {
                        longCount++;
                        long oldValue = (Long) obj;
                        editor.putLong(key, (oldValue == 0 ? 0 : r.nextLong() % oldValue));
                    } else if (obj instanceof Float) {
                        floatCount++;
                        editor.putFloat(key, r.nextFloat() * (Float) obj);
                    } else if (obj instanceof Set) {
                        //noinspection unchecked
                        Set<String> oldValue = (Set<String>) obj;
                        int count = 0;
                        Set<String> newValue = new LinkedHashSet<>();
                        for (String str : oldValue) {
                            count += str.length();
                            newValue.add(transferString(str));
                        }
                        if(count > LENGTH_LIMIT){
                            continue;
                        }
                        stringSetCount++;
                        editor.putStringSet(key, newValue);
                    }
                }
            }
            editor.apply();

            Log.i(TAG, "boolean:" + booleanCount
                    + " int:" + intCount
                    + " long:" + longCount
                    + " float:" + floatCount
                    + " string:" + stringCount
                    + " string_set: " + stringSetCount
            );
        } catch (Exception e) {
            Log.e(TAG, e.getMessage(), e);
        }
    }
}
