
package io.fastkv.fastkvdemo.sqlitekv;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.fastkv.FastKV;
import io.fastkv.fastkvdemo.base.AppContext;
import io.fastkv.fastkvdemo.util.IOUtil;
import io.packable.PackDecoder;
import io.packable.PackEncoder;

public final class SQLiteKV {
    // For boolean/int/long/float/double
    private static final String NUMBER_TABLE_NAME = "number";

    // For String
    private static final String STRING_TABLE_NAME = "string";

    // For Set<String> or other type in future
    private static final String BLOB_TABLE_NAME = "var";

    public final SQLiteDatabase db;
    private final String name;

    private SQLiteKV(String name) {
        this.name = name;
        //noinspection resource
        KVOpenHelper openHelper = new KVOpenHelper(AppContext.INSTANCE.getContext());
        db = openHelper.getWritableDatabase();
    }

    public void clear() {
        db.execSQL("DELETE from " + NUMBER_TABLE_NAME);
        db.execSQL("DELETE from " + STRING_TABLE_NAME);
        db.execSQL("DELETE from " + BLOB_TABLE_NAME);
    }

    public static void close(String name) {
        SQLiteKV kv = Builder.INSTANCE_MAP.get(name);
        if (kv != null) {
            kv.db.close();
            Builder.INSTANCE_MAP.remove(name);
        }
    }

    public static final class Builder {
        static final Map<String, SQLiteKV> INSTANCE_MAP = new ConcurrentHashMap<>();

        private final String name;

        public Builder(String name) {
            this.name = name;
        }

        public SQLiteKV build() {
            String key = name;
            SQLiteKV kv = INSTANCE_MAP.get(key);
            if (kv == null) {
                synchronized (FastKV.Builder.class) {
                    kv = INSTANCE_MAP.get(key);
                    if (kv == null) {
                        kv = new SQLiteKV(name);
                        INSTANCE_MAP.put(key, kv);
                    }
                }
            }
            return kv;
        }
    }

    private class KVOpenHelper extends SQLiteOpenHelper {
        private static final int DB_VERSION = 1;

        public KVOpenHelper(Context context) {
            super(context, name + ".db", null, DB_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase sqLiteDatabase) {
            sqLiteDatabase.execSQL("CREATE TABLE IF NOT EXISTS " + NUMBER_TABLE_NAME + " (k TEXT UNIQUE ON CONFLICT REPLACE, v INTEGER)");
            sqLiteDatabase.execSQL("CREATE TABLE IF NOT EXISTS " + STRING_TABLE_NAME + " (k TEXT UNIQUE ON CONFLICT REPLACE, v TEXT)");
            sqLiteDatabase.execSQL("CREATE TABLE IF NOT EXISTS " + BLOB_TABLE_NAME + " (k TEXT UNIQUE ON CONFLICT REPLACE, v BLOB)");
        }

        @Override
        public void onUpgrade(SQLiteDatabase sqLiteDatabase, int oldVersion, int newVersion) {
        }
    }

    public boolean putBoolean(String key, boolean value) {
        return putLong(key, value ? 1L : 0L);
    }

    public boolean getBoolean(String key, boolean defValue) {
        return getLong(key, defValue ? 1L : 0L) == 1L;
    }

    public boolean putFloat(String key, float value) {
        return putInt(key, Float.floatToRawIntBits(value));
    }

    public float getFloat(String key) {
        return Float.intBitsToFloat(getInt(key, 0));
    }

    public boolean putDouble(String key, double value) {
        return putLong(key, Double.doubleToRawLongBits(value));
    }

    public double getDouble(String key) {
        return Double.longBitsToDouble(getLong(key, 0L));
    }

    public boolean putInt(String key, int value) {
        return putLong(key, (long) value & 0xFFFFFFFFL);
    }

    public int getInt(String key, int defValue) {
        return (int) getLong(key, defValue & 0xFFFFFFFFL);
    }

    public boolean putLong(String key, long value) {
        ContentValues contentValues = new ContentValues();
        contentValues.put("k", key);
        contentValues.put("v", value);
        long rowID = db.insert(NUMBER_TABLE_NAME, null, contentValues);
        return rowID != -1;
    }

    public long getLong(String key, long defValue) {
        Cursor cursor = db.rawQuery("SELECT v FROM " + NUMBER_TABLE_NAME + " WHERE k=?", new String[]{key});
        try {
            if (cursor.moveToFirst()) {
                return cursor.getLong(0);
            }
        } finally {
            IOUtil.closeQuietly(cursor);
        }
        return defValue;
    }

    public boolean putString(String key, String value) {
        ContentValues contentValues = new ContentValues();
        contentValues.put("k", key);
        contentValues.put("v", value);
        long rowID = db.insert(STRING_TABLE_NAME, null, contentValues);
        return rowID != -1;
    }

    public String getString(String key, String defValue) {
        Cursor cursor = db.rawQuery("SELECT v FROM " + STRING_TABLE_NAME + " WHERE k=?", new String[]{key});
        try {
            if (cursor.moveToFirst()) {
                return cursor.getString(0);
            }
        } finally {
            IOUtil.closeQuietly(cursor);
        }
        return defValue;
    }

    public boolean putStringSet(String key, Set<String> value) {
        byte[] bytes = new PackEncoder().putStringList(0, value).getBytes();
        ContentValues contentValues = new ContentValues();
        contentValues.put("k", key);
        contentValues.put("v", bytes);
        long rowID = db.insert(BLOB_TABLE_NAME, null, contentValues);
        return rowID != -1;
    }

    public Set<String> getStringSet(String key, Set<String> defValues) {
        Cursor cursor = db.rawQuery("SELECT v FROM " + BLOB_TABLE_NAME + " WHERE k=?", new String[]{key});
        try {
            if (cursor.moveToFirst()) {
                byte[] bytes = cursor.getBlob(0);
                if (bytes != null) {
                    return new HashSet<>(PackDecoder.newInstance(bytes).getStringList(0));
                }
            }
        } finally {
            IOUtil.closeQuietly(cursor);
        }
        return defValues;
    }
}