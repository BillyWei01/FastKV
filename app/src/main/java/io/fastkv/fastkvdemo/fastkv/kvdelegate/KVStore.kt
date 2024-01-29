package io.fastkv.fastkvdemo.fastkv.kvdelegate

import io.fastkv.interfaces.FastEncoder

/**
 * KV存储接口
 *
 * 抽象来看，访问KV存储，和访问[Map]是类似的。
 * 常用的API，包括 put，get，remove, contains等。
 * 而 put(key, null) 等价于 remove(key) ；
 * get(key) != null 等价于 contains(key) == true。
 * 故此，我们可以简化接口，省略 remove 和 contains 。
 *
 * 另外，虽然说 get(key) ?: defValue 等价于 get(key, defValue),
 * 但是对于基础类型, 如果要实现不存在value时返回null, 需要先调用存储API contains 判断value是否存在。
 * 基于效率的考虑，对于基础类型, 我们同时声明 get(key):T? 和 get(key, defValue): T 接口。
 * 而对于String, Set<String>这样的非基本类型，要实现没有value时返回null, 不需要先判断 contains，
 * 因此，getString，getStringSet等接口，只定义一个 get(key):T? 接口即可。
 */
interface KVStore {
    fun putBoolean(key: String, value: Boolean?)

    fun getBoolean(key: String): Boolean?

    fun getBoolean(key: String, defValue: Boolean): Boolean

    fun putInt(key: String, value: Int?)

    fun getInt(key: String): Int?

    fun getInt(key: String, defValue: Int): Int

    fun putFloat(key: String, value: Float?)

    fun getFloat(key: String): Float?

    fun getFloat(key: String, defValue: Float): Float

    fun putLong(key: String, value: Long?)

    fun getLong(key: String): Long?

    fun getLong(key: String, defValue: Long): Long

    fun putDouble(key: String, value: Double?)

    fun getDouble(key: String): Double?

    fun getDouble(key: String, defValue: Double): Double

    fun putString(key: String, value: String?)

    fun getString(key: String): String?

    fun putStringSet(key: String, value: Set<String>?)

    fun getStringSet(key: String): Set<String>?

    fun putArray(key: String, value: ByteArray?)

    fun getArray(key: String): ByteArray?

    fun <T> putObject(key: String, value: T?, encoder: FastEncoder<T>)

    fun <T> getObject(key: String): T?
}