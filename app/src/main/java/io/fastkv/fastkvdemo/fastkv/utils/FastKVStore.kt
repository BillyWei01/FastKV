package io.fastkv.fastkvdemo.fastkv.utils

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.fastkv.kvdelegate.KVStore
import io.fastkv.fastkvdemo.fastkv.kvdelegate.NullableObjectEncoder
import io.fastkv.fastkvdemo.fastkv.kvdelegate.ObjectEncoder
import io.fastkv.interfaces.FastEncoder

class FastKVStore(val kv: FastKV) : KVStore {
    override fun putBoolean(key: String, value: Boolean?) {
        if (value == null) {
            kv.remove(key)
        } else {
            kv.putBoolean(key, value)
        }
    }

    override fun getBoolean(key: String): Boolean? {
        return if (kv.contains(key)) kv.getBoolean(key) else null
    }

    override fun getBoolean(key: String, defValue: Boolean): Boolean {
        return kv.getBoolean(key, defValue)
    }

    override fun putInt(key: String, value: Int?) {
        if (value == null) {
            kv.remove(key)
        } else {
            kv.putInt(key, value)
        }
    }

    override fun getInt(key: String): Int? {
        return if (kv.contains(key)) kv.getInt(key) else null
    }

    override fun getInt(key: String, defValue: Int): Int {
        return kv.getInt(key, defValue)
    }

    override fun putFloat(key: String, value: Float?) {
        if (value == null) {
            kv.remove(key)
        } else {
            kv.putFloat(key, value)
        }
    }

    override fun getFloat(key: String): Float? {
        return if (kv.contains(key)) kv.getFloat(key) else null
    }

    override fun getFloat(key: String, defValue: Float): Float {
        return kv.getFloat(key, defValue)
    }

    override fun putLong(key: String, value: Long?) {
        if (value == null) {
            kv.remove(key)
        } else {
            kv.putLong(key, value)
        }
    }

    override fun getLong(key: String): Long? {
        return if (kv.contains(key)) kv.getLong(key) else null
    }

    override fun getLong(key: String, defValue: Long): Long {
        return kv.getLong(key, defValue)
    }

    override fun putDouble(key: String, value: Double?) {
        if (value == null) {
            kv.remove(key)
        } else {
            kv.putDouble(key, value)
        }
    }

    override fun getDouble(key: String): Double? {
        return if (kv.contains(key)) kv.getDouble(key) else null
    }

    override fun getDouble(key: String, defValue: Double): Double {
        return kv.getDouble(key, defValue)
    }

    override fun putString(key: String, value: String?) {
        kv.putString(key, value)
    }

    override fun getString(key: String): String? {
        return kv.getString(key, null)
    }

    override fun putArray(key: String, value: ByteArray?) {
        kv.putArray(key, value)
    }

    override fun getArray(key: String): ByteArray? {
        return kv.getArray(key, null)
    }


    override fun putStringSet(key: String, value: Set<String>?) {
        kv.putStringSet(key, value)
    }

    override fun getStringSet(key: String): Set<String>? {
       return kv.getStringSet(key, null)
    }

    override fun <T> putObject(key: String, value: T, encoder: ObjectEncoder<T>) {
        kotlin.runCatching { putArray(key, encoder.encode(value)) }
    }

    override fun <T> getObject(key: String, encoder: ObjectEncoder<T>, defValue: T): T {
        val bytes = getArray(key)
        return if (bytes == null)
            defValue
        else
            kotlin.runCatching { encoder.decode(bytes) }.getOrDefault(defValue)
    }

    override fun <T> putNullableObject(key: String, value: T?, encoder: NullableObjectEncoder<T>) {
        kotlin.runCatching { putArray(key, encoder.encode(value)) }
    }

    override fun <T> getNullableObject(key: String, encoder: NullableObjectEncoder<T>): T? {
        return kotlin.runCatching { encoder.decode(getArray(key)) }.getOrNull()
    }
}
