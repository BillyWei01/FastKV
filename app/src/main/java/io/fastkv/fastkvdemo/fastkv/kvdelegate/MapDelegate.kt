package io.fastkv.fastkvdemo.fastkv.kvdelegate

import io.fastkv.fastkvdemo.util.LazyInitWrapper
import io.fastkv.interfaces.FastEncoder
import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KProperty


//------------------------“两级Key”属性委托的实现-------------------------

private class LazyInitializer<T>(initializer: (KVData) -> T) : LazyInitWrapper<KVData, T>(initializer)

class CombineKeyProperty(private val preKey: String) : ReadOnlyProperty<KVData, CombineKV> {
    private val combineKV = LazyInitializer { CombineKV(preKey, it) }

    override fun getValue(thisRef: KVData, property: KProperty<*>): CombineKV {
        return combineKV.get(thisRef)
    }
}

class CombineKV(private val preKey: String, private val kvData: KVData) {
    private fun combineKey(key: String): String {
        return "${preKey}__${key}"
    }

    fun containsKey(key: String): Boolean {
        return kvData.kv.contains(combineKey(key))
    }

    fun remove(key: String) {
        kvData.kv.remove(combineKey(key))
    }

    fun putBoolean(key: String, value: Boolean) {
        kvData.kv.putBoolean(combineKey(key), value)
    }

    fun getBoolean(key: String, defValue: Boolean = false): Boolean {
        return kvData.kv.getBoolean(combineKey(key), defValue)
    }

    fun putInt(key: String, value: Int) {
        kvData.kv.putInt(combineKey(key), value)
    }

    fun getInt(key: String, defValue: Int = 0): Int {
        return kvData.kv.getInt(combineKey(key), defValue)
    }

    fun putFloat(key: String, value: Float) {
        kvData.kv.putFloat(combineKey(key), value)
    }

    fun getFloat(key: String, defValue: Float = 0f): Float {
        return kvData.kv.getFloat(combineKey(key), defValue)
    }

    fun putLong(key: String, value: Long) {
        kvData.kv.putLong(combineKey(key), value)
    }

    fun getLong(key: String, defValue: Long = 0L): Long {
        return kvData.kv.getLong(combineKey(key), defValue)
    }

    fun putDouble(key: String, value: Double) {
        kvData.kv.putDouble(combineKey(key), value)
    }

    fun getDouble(key: String, defValue: Double = 0.0): Double {
        return kvData.kv.getDouble(combineKey(key), defValue)
    }

    fun putString(key: String, value: String?) {
        kvData.kv.putString(combineKey(key), value)
    }

    fun getString(key: String, defValue: String? = null): String? {
        return kvData.kv.getString(combineKey(key), null) ?: defValue
    }

    fun putArray(key: String, value: ByteArray?) {
        kvData.kv.putArray(combineKey(key), value)
    }

    fun getArray(key: String, defValue: ByteArray? = null): ByteArray? {
        return kvData.kv.getArray(combineKey(key), defValue)
    }

    fun putStringSet(key: String, value: Set<String>?) {
        kvData.kv.putStringSet(combineKey(key), value)
    }

    fun getStringSet(key: String): Set<String>? {
        return kvData.kv.getStringSet(combineKey(key))
    }

    fun <T> putObject(key: String, value: T, encoder: FastEncoder<T>) {
        kvData.kv.putObject(combineKey(key), value, encoder)
    }

    fun <T> getObject(key: String): T? {
        return kvData.kv.getObject(combineKey(key))
    }
}


//------------------------Map类型属性委托的实现-------------------------

open class KVMapper(protected val combineKV: CombineKV) {
    fun contains(key: String): Boolean {
        return combineKV.containsKey(key)
    }

    fun remove(key: String) {
        combineKV.remove(key)
    }
}

// -------------------------------------------------

class StringToStringProperty(private val preKey: String) : ReadOnlyProperty<KVData, String2String> {
    private val mapper = LazyInitializer { String2String(CombineKV(preKey, it)) }

    override fun getValue(thisRef: KVData, property: KProperty<*>): String2String {
        return mapper.get(thisRef)
    }
}

class String2String(combineKV: CombineKV) : KVMapper(combineKV) {
    operator fun get(key: String): String? {
        return combineKV.getString(key, null)
    }

    operator fun set(key: String, value: String?) {
        combineKV.putString(key, value)
    }
}

// -------------------------------------------------

class StringToSetProperty(preKey: String) : ReadOnlyProperty<KVData, String2Set> {
    private val mapper = LazyInitializer { String2Set(CombineKV(preKey, it)) }

    override fun getValue(thisRef: KVData, property: KProperty<*>): String2Set {
        return mapper.get(thisRef)
    }
}

class String2Set(combineKV: CombineKV) : KVMapper(combineKV) {
    operator fun get(key: String): Set<String>? {
        return combineKV.getStringSet(key)
    }

    operator fun set(key: String, value: Set<String>?) {
        combineKV.putStringSet(key, value)
    }
}

// -------------------------------------------------

class StringToIntProperty(preKey: String) : ReadOnlyProperty<KVData, String2Int> {
    private val mapper = LazyInitializer { String2Int(CombineKV(preKey, it)) }

    override fun getValue(thisRef: KVData, property: KProperty<*>): String2Int {
        return mapper.get(thisRef)
    }
}

class String2Int(combineKV: CombineKV) : KVMapper(combineKV) {
    operator fun get(key: String): Int {
        return combineKV.getInt(key)
    }

    operator fun set(key: String, value: Int) {
        combineKV.putInt(key, value)
    }
}

// -------------------------------------------------

class StringToBooleanProperty(preKey: String) : ReadOnlyProperty<KVData, String2Boolean> {
    private val mapper = LazyInitializer { String2Boolean(CombineKV(preKey, it)) }

    override fun getValue(thisRef: KVData, property: KProperty<*>): String2Boolean {
        return mapper.get(thisRef)
    }
}

class String2Boolean(combineKV: CombineKV) : KVMapper(combineKV) {
    operator fun get(key: String): Boolean {
        return combineKV.getBoolean(key)
    }

    operator fun set(key: String, value: Boolean) {
        combineKV.putBoolean(key, value)
    }
}

// -------------------------------------------------

class IntToBooleanProperty(preKey: String) : ReadOnlyProperty<KVData, Int2Boolean> {
    private val mapper = LazyInitializer { Int2Boolean(CombineKV(preKey, it)) }

    override fun getValue(thisRef: KVData, property: KProperty<*>): Int2Boolean {
        return mapper.get(thisRef)
    }
}

class Int2Boolean(combineKV: CombineKV) : KVMapper(combineKV) {
    operator fun get(key: Int): Boolean {
        return combineKV.getBoolean(key.toString())
    }

    operator fun set(key: Int, value: Boolean) {
        combineKV.putBoolean(key.toString(), value)
    }
}