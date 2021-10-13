package io.github.fastkvdemo.fastkv;

import android.content.Context;
import android.content.SharedPreferences;

import androidx.annotation.Nullable;

import java.io.File;
import java.util.Map;
import java.util.Set;

import io.fastkv.FastKV;

public class FastPreferences implements SharedPreferences {
    private static final String IMPORT_FLAG = "import_flag";

    protected final FastKV kv;
    protected final FastEditor editor = new FastEditor();

    public FastPreferences(String path, String name) {
        kv = new FastKV.Builder(path, name).build();
    }

    /**
     * Adapt old SharePreferences,
     * return a new SharedPreferences with storage strategy of FastKV.
     *
     * Node: The old SharePreferences must implement getAll() method,
     * otherwise can not import old data to new files.
     *
     * @param context The context
     * @param name The name of SharePreferences
     * @param deleteOldData If set true, delete old data after import
     * @return The Wrapper of FastKV, which implement SharePreferences.
     */
    public static SharedPreferences adapt(Context context, String name, boolean deleteOldData) {
        String path = context.getFilesDir().getAbsolutePath() + "/fastkv";
        FastPreferences newPreferences = new FastPreferences(path, name);
        if (!newPreferences.contains(IMPORT_FLAG)) {
            SharedPreferences oldPreferences = context.getSharedPreferences(name, Context.MODE_PRIVATE);
            //noinspection unchecked
            Map<String, Object> allData = (Map<String, Object>) oldPreferences.getAll();
            FastKV kv = newPreferences.getKV();
            kv.putAll(allData);
            kv.putBoolean(IMPORT_FLAG, true);
            if (deleteOldData) {
                oldPreferences.edit().clear().apply();
            }
        }
        return newPreferences;
    }

    public FastKV getKV() {
        return kv;
    }

    @Override
    public Map<String, ?> getAll() {
        return kv.getAll();
    }

    @Nullable
    @Override
    public String getString(String key, @Nullable String defValue) {
        return kv.getString(key, defValue);
    }

    @Nullable
    @Override
    public Set<String> getStringSet(String key, @Nullable Set<String> defValues) {
        Set<String> value = kv.getStringSet(key);
        return value != null ? value : defValues;
    }

    @Override
    public int getInt(String key, int defValue) {
        return kv.getInt(key, defValue);
    }

    @Override
    public long getLong(String key, long defValue) {
        return kv.getLong(key, defValue);
    }

    @Override
    public float getFloat(String key, float defValue) {
        return kv.getFloat(key, defValue);
    }

    @Override
    public boolean getBoolean(String key, boolean defValue) {
        return kv.getBoolean(key, defValue);
    }

    @Override
    public boolean contains(String key) {
        return kv.contains(key);
    }

    @Override
    public Editor edit() {
        return editor;
    }

    @Override
    public void registerOnSharedPreferenceChangeListener(OnSharedPreferenceChangeListener listener) {

    }

    @Override
    public void unregisterOnSharedPreferenceChangeListener(OnSharedPreferenceChangeListener listener) {

    }

    private class FastEditor implements SharedPreferences.Editor {
        @Override
        public Editor putString(String key, @Nullable String value) {
            kv.putString(key, value);
            return this;
        }

        @Override
        public Editor putStringSet(String key, @Nullable Set<String> values) {
            kv.putStringSet(key, values);
            return this;
        }

        @Override
        public Editor putInt(String key, int value) {
            kv.putInt(key, value);
            return this;
        }

        @Override
        public Editor putLong(String key, long value) {
            kv.putLong(key, value);
            return this;
        }

        @Override
        public Editor putFloat(String key, float value) {
            kv.putFloat(key, value);
            return this;
        }

        @Override
        public Editor putBoolean(String key, boolean value) {
            kv.putBoolean(key, value);
            return this;
        }

        @Override
        public Editor remove(String key) {
            kv.remove(key);
            return this;
        }

        @Override
        public Editor clear() {
            kv.clear();
            return this;
        }

        @Override
        public boolean commit() {
            return true;
        }

        @Override
        public void apply() {
        }
    }
}
