package io.fastkv.fastkvdemo.data

import com.tencent.mmkv.MMKV
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.base.AppContext

/**
 * MMKV 迁移 FastKV 的样例
 */
class FooKV(val name: String) {
    // Using lazy initialization.
    // If all key-values can be fetched from fastkv, no need to initialize mmkv.
    private val mmkv by lazy {
        MMKV.mmkvWithID(name)
    }

    private val fastkv = FastKV.Builder(AppContext.context, name).build()

    fun putString(key: String, value: String) {
        // Unnecessary to considering old value when putting new value.
        // Just put it (add or replace).
        fastkv.putString(key, value)
    }

    fun getString(key: String): String? {
        // option 1:
        /*
        if (!fastkv.contains(key)) {
            val value = mmkv.decodeString(key)
            if (value != null) {
                fastkv.putString(key, value)
            }
            return value
        }
        return fastkv.getString(key)
        */

        // option 2 (Better):
        // Try to get value from new kv at first, if miss, checking old kv.
        var value = fastkv.getString(key)
        if (value != null) {
            return value
        }
        value = mmkv.decodeString(key)
        if (value != null) {
            fastkv.putString(key, value)
        }
        return value
    }

    fun putInt(key: String, value: Int) {
        fastkv.putInt(key, value)
    }

    fun getInt(key: String): Int {
        // Int value doesn't hava null state ('0' does not mean there is no value).
        // So use 'contains' to check if there is value.
        if (!fastkv.contains(key)) {
            val value = mmkv.decodeInt(key)
            fastkv.putInt(key, value)
            return value
        }
        return fastkv.getInt(key)
    }
}
