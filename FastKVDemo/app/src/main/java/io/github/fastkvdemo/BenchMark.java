package io.github.fastkvdemo;

import android.content.Context;
import android.content.SharedPreferences;
import android.util.Log;
import android.util.Pair;

import com.tencent.mmkv.MMKV;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import io.fastkv.FastKV;
import io.github.fastkvdemo.application.GlobalConfig;
import io.github.fastkvdemo.manager.PathManager;
import io.github.fastkvdemo.util.IOUtil;
import io.github.fastkvdemo.util.Utils;

public class BenchMark {
    private static final String TAG = "Benchmark";

    private static final SharedPreferences sp = GlobalConfig.appContext.getSharedPreferences("sp", Context.MODE_PRIVATE);
    private static final MMKV mmkv = MMKV.defaultMMKV();
    private static final FastKV fastkv = new FastKV.Builder(PathManager.INSTANCE.getFastKVDir(), "fastkv").build();

    private static final int MILLION = 1000000;

    private static boolean hadWarmingUp = false;

    public static void start() {
        try {
            runTest(GlobalConfig.appContext);
        } catch (Throwable e) {
            Log.e(TAG, e.getMessage(), e);
        }
    }

    private static void runTest(Context context) throws InterruptedException, IOException {
        Random r = new Random(1);

        ArrayList<Pair<String, Object>> srcList = generateInputList(loadSourceData(context));
        warmingUp(srcList, r);

        List<Pair<String, Object>> inputList;

        Log.i(TAG, "Clear data");
        sp.edit().clear().apply();
        mmkv.clear();
        fastkv.clear();

        Log.i(TAG, "Start test");

        long[] time = new long[3];
        Arrays.fill(time, 0L);
        inputList = new ArrayList<>(srcList);
        for (int i = 0; i < 1; i++) {
            long t1 = System.nanoTime();
            applyToSp(inputList);
            long t2 = System.nanoTime();
            putToMMKV(inputList);
            long t3 = System.nanoTime();
            putToFastKV(inputList);
            long t4 = System.nanoTime();
            time[0] += (t2 - t1);
            time[1] += (t3 - t2);
            time[2] += (t4 - t3);
        }
        Log.i(TAG, "Fill, sp: " + time[0] / MILLION + ", mmkv: " + time[1] / MILLION + ", fastkv:" + time[2] / MILLION);


        Arrays.fill(time, 0L);
        int round = 10;

        for (int i = 0; i < round; i++) {
            inputList = getDistributedList(srcList, r);
            long t1 = System.nanoTime();
            applyToSp(inputList);
            long t2 = System.nanoTime();
            putToMMKV(inputList);
            long t3 = System.nanoTime();
            putToFastKV(inputList);
            long t4 = System.nanoTime();
            long a = t2 - t1;
            long b = t3 - t2;
            long c = t4 - t3;
            time[0] += a;
            time[1] += b;
            time[2] += c;
            Log.d(TAG, "Update, sp: " + a / MILLION + ", mmkv: " + b / MILLION + ", fastkv:" + c / MILLION);
        }
        Log.i(TAG, "Update total time, sp: " + time[0] / MILLION + ", mmkv: " + time[1] / MILLION + ", fastkv:" + time[2] / MILLION);

        Arrays.fill(time, 0L);
        for (int i = 0; i < round; i++) {
            long t1 = System.nanoTime();
            readFromSp(inputList);
            long t2 = System.nanoTime();
            readFromMMKV(inputList);
            long t3 = System.nanoTime();
            readFromFastKV(inputList);
            long t4 = System.nanoTime();
            time[0] += t2 - t1;
            time[1] += t3 - t2;
            time[2] += t4 - t3;
        }
        Log.i(TAG, "Read total time, sp: " + time[0] / MILLION + ", mmkv: " + time[1] / MILLION + ", fastkv:" + time[2] / MILLION);
    }

    private static void warmingUp(ArrayList<Pair<String, Object>> srcList, Random r) throws InterruptedException {
        if (!hadWarmingUp) {
            for (int i = 0; i < 1; i++) {
                applyToSp(srcList);
                putToMMKV(srcList);
                putToFastKV(srcList);
            }
            for (int i = 0; i < 10; i++) {
                List<Pair<String, Object>> inputList = getDistributedList(srcList, r);
                applyToSp(inputList);
                putToMMKV(inputList);
                putToFastKV(inputList);
            }
            hadWarmingUp = true;
            Thread.sleep(50L);
        }
    }

    private static Map<String, ?> loadSourceData(Context context) throws IOException {
        String srcName = "sum";
        File spDir = new File(context.getFilesDir().getParent(), "/shared_prefs");
        File sumFile = new File(spDir, srcName + ".xml");
        if (!sumFile.exists()) {
            byte[] bytes = IOUtil.streamToBytes(context.getAssets().open(srcName + ".xml"));
            IOUtil.bytesToFile(bytes, sumFile);
        }

        SharedPreferences sumPreferences = context.getSharedPreferences(srcName, Context.MODE_PRIVATE);
        return sumPreferences.getAll();
    }


    private static List<Pair<String, Object>> getDistributedList(List<Pair<String, Object>> srcList, Random r) {
        List<Pair<String, Object>> inputList = new ArrayList<>(srcList);
        int[] a = Utils.getDistributedArray(srcList.size(), 10, r);
        for (int index : a) {
            Pair<String, Object> pair = srcList.get(index);
            inputList.add(new Pair<>(pair.first, tuningObject(pair.second, r)));
        }
        return inputList;
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

    private static ArrayList<Pair<String, Object>> generateInputList(Map<String, ?> all) {
        ArrayList<Pair<String, Object>> list = new ArrayList<>(all.size());
        for (Map.Entry<String, ?> entry : all.entrySet()) {
            list.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        return list;
    }

    private static void applyToSp(List<Pair<String, Object>> list) {
        SharedPreferences.Editor editor = sp.edit();
        for (Pair<String, Object> pair : list) {
            String key = pair.first;
            Object value = pair.second;
            if (value instanceof String) {
                editor.putString(key, (String) value).apply();
            } else if (value instanceof Boolean) {
                editor.putBoolean(key, (Boolean) value).apply();
            } else if (value instanceof Integer) {
                editor.putInt(key, (Integer) value).apply();
            } else if (value instanceof Long) {
                editor.putLong(key, (Long) value).apply();
            } else if (value instanceof Float) {
                editor.putFloat(key, (Float) value).apply();
            } else if (value instanceof Set) {
                //noinspection unchecked
                editor.putStringSet(key, (Set<String>) value).apply();
            }
        }
    }

    private static void readFromSp(List<Pair<String, Object>> list) {
        for (Pair<String, Object> pair : list) {
            String key = pair.first;
            Object value = pair.second;
            if (value instanceof String) {
                sp.getString(key, "");
            } else if (value instanceof Boolean) {
                sp.getBoolean(key, false);
            } else if (value instanceof Integer) {
                sp.getInt(key, 0);
            } else if (value instanceof Long) {
                sp.getLong(key, 0L);
            } else if (value instanceof Float) {
                sp.getFloat(key, 0);
            } else if (value instanceof Set) {
                sp.getStringSet(key, null);
            }
        }
    }

    private static void putToMMKV(List<Pair<String, Object>> list) {
        for (Pair<String, Object> pair : list) {
            String key = pair.first;
            Object value = pair.second;
            if (value instanceof String) {
                mmkv.putString(key, (String) value);
            } else if (value instanceof Boolean) {
                mmkv.putBoolean(key, (Boolean) value);
            } else if (value instanceof Integer) {
                mmkv.putInt(key, (Integer) value);
            } else if (value instanceof Long) {
                mmkv.putLong(key, (Long) value);
            } else if (value instanceof Float) {
                mmkv.putFloat(key, (Float) value);
            } else if (value instanceof Set) {
                //noinspection unchecked
                mmkv.putStringSet(key, (Set<String>) value);
            }
        }
    }

    private static void readFromMMKV(List<Pair<String, Object>> list) {
        for (Pair<String, Object> pair : list) {
            String key = pair.first;
            Object value = pair.second;
            if (value instanceof String) {
                mmkv.getString(key, "");
            } else if (value instanceof Boolean) {
                mmkv.getBoolean(key, false);
            } else if (value instanceof Integer) {
                mmkv.getInt(key, 0);
            } else if (value instanceof Long) {
                mmkv.getLong(key, 0L);
            } else if (value instanceof Float) {
                mmkv.getFloat(key, 0);
            } else if (value instanceof Set) {
                mmkv.getStringSet(key, null);
            }
        }
    }

    private static void putToFastKV(List<Pair<String, Object>> list) {
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

    private static void readFromFastKV(List<Pair<String, Object>> list) {
        for (Pair<String, Object> pair : list) {
            String key = pair.first;
            Object value = pair.second;
            if (value instanceof String) {
                fastkv.getString(key, "");
            } else if (value instanceof Boolean) {
                fastkv.getBoolean(key, false);
            } else if (value instanceof Integer) {
                fastkv.getInt(key, 0);
            } else if (value instanceof Long) {
                fastkv.getLong(key, 0L);
            } else if (value instanceof Float) {
                fastkv.getFloat(key, 0);
            } else if (value instanceof Set) {
                fastkv.getStringSet(key);
            }
        }
    }

}
