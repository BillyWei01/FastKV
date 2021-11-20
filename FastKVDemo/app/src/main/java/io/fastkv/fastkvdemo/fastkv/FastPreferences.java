package io.fastkv.fastkvdemo.fastkv;

import androidx.annotation.Nullable;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Handler;
import android.os.Looper;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import io.fastkv.FastKV;

public class FastPreferences implements SharedPreferences {
    protected final FastKV kv;
    protected final FastEditor editor = new FastEditor();

    private static class HandlerHolder {
        private static final Handler handler = new Handler(Looper.getMainLooper());
    }

    private final Object mLock = new Object();
    private final ArrayList<OnSharedPreferenceChangeListener> listeners = new ArrayList<>();

    public FastPreferences(String path, String name) {
        kv = new FastKV.Builder(path, name).build();
    }

    /**
     * Adapt old SharePreferences,
     * return a new SharedPreferences with storage strategy of FastKV.
     * <p>
     * Node: The old SharePreferences must implement getAll() method,
     * otherwise can not import old data to new files.
     *
     * @param context       The context
     * @param name          The name of SharePreferences
     * @param deleteOldData If set true, delete old data after import
     * @return The Wrapper of FastKV, which implement SharePreferences.
     */
    public static SharedPreferences adapt(Context context, String name, boolean deleteOldData) {
        String path = context.getFilesDir().getAbsolutePath() + "/fastkv";
        FastPreferences newPreferences = new FastPreferences(path, name);
        final String flag = "kv_import_flag";
        if (!newPreferences.contains(flag)) {
            SharedPreferences oldPreferences = context.getSharedPreferences(name, Context.MODE_PRIVATE);
            //noinspection unchecked
            Map<String, Object> allData = (Map<String, Object>) oldPreferences.getAll();
            FastKV kv = newPreferences.getKV();
            kv.putAll(allData);
            kv.putBoolean(flag, true);
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
        if (listener == null) {
            return;
        }
        synchronized (mLock) {
            if (!listeners.contains(listener)) {
                listeners.add(listener);
            }
        }
    }

    @Override
    public void unregisterOnSharedPreferenceChangeListener(OnSharedPreferenceChangeListener listener) {
        synchronized (mLock) {
            listeners.remove(listener);
        }
    }

    private void notifyChanged(String key) {
        synchronized (mLock) {
            if (!listeners.isEmpty()) {
                if (Looper.myLooper() == Looper.getMainLooper()) {
                    notifyListeners(listeners, key);
                } else {
                    HandlerHolder.handler.post(() -> {
                        synchronized (mLock) {
                            notifyListeners(listeners, key);
                        }
                    });
                }
            }
        }
    }

    private void notifyListeners(ArrayList<OnSharedPreferenceChangeListener> listeners, String key) {
        for (OnSharedPreferenceChangeListener listener : listeners) {
            listener.onSharedPreferenceChanged(this, key);
        }
    }

    private class FastEditor implements SharedPreferences.Editor {
        @Override
        public Editor putString(String key, @Nullable String value) {
            kv.putString(key, value);
            notifyChanged(key);
            return this;
        }

        @Override
        public Editor putStringSet(String key, @Nullable Set<String> values) {
            kv.putStringSet(key, values);
            notifyChanged(key);
            return this;
        }

        @Override
        public Editor putInt(String key, int value) {
            kv.putInt(key, value);
            notifyChanged(key);
            return this;
        }

        @Override
        public Editor putLong(String key, long value) {
            kv.putLong(key, value);
            notifyChanged(key);
            return this;
        }

        @Override
        public Editor putFloat(String key, float value) {
            kv.putFloat(key, value);
            notifyChanged(key);
            return this;
        }

        @Override
        public Editor putBoolean(String key, boolean value) {
            kv.putBoolean(key, value);
            notifyChanged(key);
            return this;
        }

        @Override
        public Editor remove(String key) {
            kv.remove(key);
            notifyChanged(key);
            return this;
        }

        @Override
        public Editor clear() {
            kv.clear();
            notifyChanged(null);
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
