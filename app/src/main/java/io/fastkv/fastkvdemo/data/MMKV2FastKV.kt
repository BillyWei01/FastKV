package io.fastkv.fastkvdemo.data

import com.tencent.mmkv.MMKV
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.fastkv.kvdelegate.KVStore
import io.fastkv.fastkvdemo.fastkv.kvdelegate.ObjectConvertor

/**
 *
 * MMKV 迁移 FastKV 的样例
 *
 * 基本思路：
 * 1. 更新：直接写入到FastKV。
 * 2. 查询：先查看FastKV有没有值，如果没有，再查看MMKV,
 *         如果MMKV有值，则写入FastKV，并返回；
 *         如果MMKV没有值, 则返回null。
 */
@Suppress("SpellCheckingInspection")
class MMKV2FastKV(val name: String) : KVStore {
    private val mmkv by lazy { MMKV.mmkvWithID(name) }
    private val fastkv = FastKV.Builder(AppContext.context, name).build()

    override fun putBoolean(key: String, value: Boolean?) {
        if (value == null) {
            fastkv.remove(key)
        } else {
            fastkv.putBoolean(key, value)
        }
    }

    @Synchronized
    override fun getBoolean(key: String): Boolean? {
        if (!fastkv.contains(key)) {
            if (!mmkv.containsKey(key)) return null
            mmkv.decodeBool(key).also { fastkv.putBoolean(key, it) }
        }
        return fastkv.getBoolean(key)
    }

    override fun getBoolean(key: String, defValue: Boolean): Boolean {
        return getBoolean(key) ?: defValue
    }

    override fun putInt(key: String, value: Int?) {
        if (value == null) {
            fastkv.remove(key)
        } else {
            fastkv.putInt(key, value)
        }
    }

    @Synchronized
    override fun getInt(key: String): Int? {
        if (!fastkv.contains(key)) {
            if (!mmkv.containsKey(key)) return null
            mmkv.decodeInt(key).also { fastkv.putInt(key, it) }
        }
        return fastkv.getInt(key)
    }

    override fun getInt(key: String, defValue: Int): Int {
        return getInt(key) ?: defValue
    }

    override fun putFloat(key: String, value: Float?) {
        if (value == null) {
            fastkv.remove(key)
        } else {
            fastkv.putFloat(key, value)
        }
    }

    @Synchronized
    override fun getFloat(key: String): Float? {
        if (!fastkv.contains(key)) {
            if (!mmkv.containsKey(key)) return null
            mmkv.decodeFloat(key).also { fastkv.putFloat(key, it) }
        }
        return fastkv.getFloat(key)
    }

    override fun getFloat(key: String, defValue: Float): Float {
        return getFloat(key) ?: defValue
    }

    override fun putLong(key: String, value: Long?) {
        if (value == null) {
            fastkv.remove(key)
        } else {
            fastkv.putLong(key, value)
        }
    }

    @Synchronized
    override fun getLong(key: String): Long? {
        if (!fastkv.contains(key)) {
            if (!mmkv.containsKey(key)) return null
            mmkv.decodeLong(key).also { fastkv.putLong(key, it) }
        }
        return fastkv.getLong(key)
    }

    override fun getLong(key: String, defValue: Long): Long {
        return getLong(key) ?: defValue
    }

    override fun putDouble(key: String, value: Double?) {
        if (value == null) {
            fastkv.remove(key)
        } else {
            fastkv.putDouble(key, value)
        }
    }

    @Synchronized
    override fun getDouble(key: String): Double? {
        if (!fastkv.contains(key)) {
            if (!mmkv.containsKey(key)) return null
            mmkv.decodeDouble(key).also { fastkv.putDouble(key, it) }
        }
        return fastkv.getDouble(key)
    }

    override fun getDouble(key: String, defValue: Double): Double {
        return getDouble(key) ?: defValue
    }

    override fun putString(key: String, value: String?) {
        fastkv.putString(key, value)
    }

    @Synchronized
    override fun getString(key: String): String? {
        return fastkv.getString(key, null) ?: mmkv.decodeString(key)?.also {
            fastkv.putString(key, it)
        }
    }

    override fun putArray(key: String, value: ByteArray?) {
        fastkv.putArray(key, value)
    }

    @Synchronized
    override fun getArray(key: String): ByteArray? {
        return fastkv.getArray(key, null) ?: mmkv.decodeBytes(key)?.also {
            fastkv.putArray(key, it)
        }
    }

    override fun putStringSet(key: String, value: Set<String>?) {
        fastkv.putStringSet(key, value)
    }

    @Synchronized
    override fun getStringSet(key: String): Set<String>? {
        return fastkv.getStringSet(key, null) ?: mmkv.decodeStringSet(key)?.also {
            fastkv.putStringSet(key, it)
        }
    }

    override fun <T> putObject(key: String, value: T?, encoder: ObjectConvertor<T>) {
        if (value == null) {
            fastkv.remove(key)
        } else {
            kotlin.runCatching { putArray(key, encoder.encode(value)) }
        }
    }

    override fun <T> getObject(key: String, encoder: ObjectConvertor<T>): T? {
        val bytes = getArray(key)
        return if (bytes == null)
            null
        else
            kotlin.runCatching { encoder.decode(bytes) }.getOrNull()
    }
}