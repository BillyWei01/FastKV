package io.fastkv.fastkvdemo.fastkv.kvdelegate

import io.fastkv.interfaces.FastEncoder
import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KProperty

//------------------------扩展key的属性委托-------------------------

class CombineKVProperty(private val preKey: String) : ReadOnlyProperty<KVData, CombineKV> {
    private val combineKV = LazyInitializer { CombineKV(preKey, it) }

    override fun getValue(thisRef: KVData, property: KProperty<*>): CombineKV {
        return combineKV.get(thisRef)
    }
}

/**
 * 基本类型的属性委托，定义时需传入常量的key，而开发中有时候需要【常量+变量】的key。
 * 为此，我们定义这个[CombineKV]类。
 *
 * 此类实现了[KVStore]，在传key参数过程中，拼接本身的[key]以及参数传入的 extKey,
 * 从而实现【常量+变量】的效果。
 * 需要注意的时，当参数传入的extKey为空字符时，相当于直接用[key]访问数据。
 *
 */
@Suppress("PARAMETER_NAME_CHANGED_ON_OVERRIDE")
class CombineKV(private val key: String, private val kvData: KVData): KVStore {
    companion object {
        const val SEPARATOR = "--"
    }

    private val kv: KVStore
        get() = kvData.kv

    private fun combineKey(extKey: Any): String {
        val extKeyStr = extKey.toString()
        return if (extKeyStr.isEmpty()) key else "$key$SEPARATOR$extKeyStr"
    }

    override fun putBoolean(extKey: String, value: Boolean?) {
        kv.putBoolean(combineKey(extKey), value)
    }

    override fun getBoolean(extKey: String): Boolean? {
        return kv.getBoolean(combineKey(extKey))
    }

    override fun getBoolean(extKey: String, defValue: Boolean): Boolean {
        return kv.getBoolean(combineKey(extKey), defValue)
    }

    override fun putInt(extKey: String, value: Int?) {
        kv.putInt(combineKey(extKey), value)
    }

    override fun getInt(extKey: String): Int? {
        return kv.getInt(combineKey(extKey))
    }

    override fun getInt(extKey: String, defValue: Int): Int {
        return kv.getInt(combineKey(extKey), defValue)
    }

    override fun putFloat(extKey: String, value: Float?) {
        return kv.putFloat(combineKey(extKey), value)
    }

    override fun getFloat(extKey: String): Float? {
       return kv.getFloat(combineKey(extKey))
    }

    override fun getFloat(extKey: String, defValue: Float): Float {
      return kv.getFloat(combineKey(extKey), defValue)
    }

    override fun putLong(extKey: String, value: Long?) {
        return kv.putLong(combineKey(extKey), value)
    }

    override fun getLong(extKey: String): Long? {
        return kv.getLong(combineKey(extKey))
    }

    override fun getLong(extKey: String, defValue: Long): Long {
        return kv.getLong(combineKey(extKey), defValue)
    }

    override fun putDouble(extKey: String, value: Double?) {
        return kv.putDouble(combineKey(extKey), value)
    }

    override fun getDouble(extKey: String): Double? {
        return kv.getDouble(combineKey(extKey))
    }

    override fun getDouble(extKey: String, defValue: Double): Double {
        return kv.getDouble(combineKey(extKey), defValue)
    }

    override fun putString(extKey: String, value: String?) {
        return kv.putString(combineKey(extKey), value)
    }

    override fun getString(extKey: String): String? {
        return kv.getString(combineKey(extKey))
    }

    override fun putArray(extKey: String, value: ByteArray?) {
        return kv.putArray(combineKey(extKey), value)
    }

    override fun getArray(extKey: String): ByteArray? {
        return kv.getArray(combineKey(extKey))
    }

    override fun putStringSet(extKey: String, value: Set<String>?) {
        return kv.putStringSet(combineKey(extKey), value)
    }

    override fun getStringSet(extKey: String): Set<String>? {
        return kv.getStringSet(combineKey(extKey))
    }

    override fun <T> putObject(key: String, value: T?, encoder: FastEncoder<T>) {
        kv.putObject(key, value, encoder)
    }

    override fun <T> getObject(key: String): T? {
        return kv.getObject(key)
    }
}

private class LazyInitializer<T>(private val creator: (param: KVData) -> T) {
    @Volatile
    private var instance: T? = null

    fun get(param: KVData): T {
        val t = instance
        if (t != null) return t
        synchronized(this) {
            if (instance == null) {
                instance = creator.invoke(param)
            }
            return instance!!
        }
    }
}

//-------------------------------------------------------------------------

/**
 * CombineKV是比较灵活的API，get/set任意类型的value。
 * 但有时候我们想要约束【常量+变量】key所对应的value类型，
 * 故此，我们定义一些明确value类型的API。
 *
 * 在这些实现中，我们用 `operator` 修饰get和set方法；
 * 通过委托访问value时，语法如下：
 * ```
 * // 读取
 * val value = Foo.xxx[extKey]
 *
 * // 写入
 * Foo.xxx[extKey] = value
 * ```
 *
 * extKey可以任意类型的参数。
 * 需要指出，extKey 的 toString() 方法需返回稳定唯一的字符串。
 */
open class ExtKV(protected val combineKV: CombineKV)

open class ExtProperty<T : ExtKV>(
    private val key: String,
    private val creator: (kv: CombineKV) -> T
) : ReadOnlyProperty<KVData, T> {
    private val extKV = LazyInitializer { creator(CombineKV(key, it)) }

    override fun getValue(thisRef: KVData, property: KProperty<*>): T {
        return extKV.get(thisRef)
    }
}

//--------------------------------------------------------------------

class ExtBooleanProperty(key: String, defValue: Boolean) :
    ExtProperty<ExtBoolean>(key, { ExtBoolean(it, defValue) })

class ExtBoolean(combineKV: CombineKV, private val defValue: Boolean) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Boolean {
        return combineKV.getBoolean(extKey.toString(), defValue)
    }

    operator fun set(extKey: Any, value: Boolean) {
        combineKV.putBoolean(extKey.toString(), value)
    }
}

class ExtNullableBooleanProperty(key: String) :
    ExtProperty<ExtNullableBoolean>(key, { ExtNullableBoolean(it) })

class ExtNullableBoolean(combineKV: CombineKV) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Boolean? {
        return combineKV.getBoolean(extKey.toString())
    }

    operator fun set(extKey: Any, value: Boolean?) {
        combineKV.putBoolean(extKey.toString(), value)
    }
}

//--------------------------------------------------------------------

class ExtIntProperty(key: String, defValue: Int) :
    ExtProperty<ExtInt>(key, { ExtInt(it, defValue) })

class ExtInt(combineKV: CombineKV, private val defValue: Int) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Int {
        return combineKV.getInt(extKey.toString(), defValue)
    }

    operator fun set(extKey: Any, value: Int) {
        combineKV.putInt(extKey.toString(), value)
    }
}

class ExtNullableIntProperty(key: String) :
    ExtProperty<ExtNullableInt>(key, { ExtNullableInt(it) })

class ExtNullableInt(combineKV: CombineKV) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Int? {
        return combineKV.getInt(extKey.toString())
    }

    operator fun set(extKey: Any, value: Int?) {
        combineKV.putInt(extKey.toString(), value)
    }
}

//--------------------------------------------------------------------

class ExtFloatProperty(key: String, defValue: Float) :
    ExtProperty<ExtFloat>(key, { ExtFloat(it, defValue) })

class ExtFloat(combineKV: CombineKV, private val defValue: Float) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Float {
        return combineKV.getFloat(extKey.toString(), defValue)
    }

    operator fun set(extKey: Any, value: Float) {
        combineKV.putFloat(extKey.toString(), value)
    }
}

class ExtNullableFloatProperty(key: String) :
    ExtProperty<ExtNullableFloat>(key, { ExtNullableFloat(it) })

class ExtNullableFloat(combineKV: CombineKV) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Float? {
        return combineKV.getFloat(extKey.toString())
    }

    operator fun set(extKey: Any, value: Float?) {
        combineKV.putFloat(extKey.toString(), value)
    }
}

//--------------------------------------------------------------------

class ExtLongProperty(key: String, defValue: Long) :
    ExtProperty<ExtLong>(key, { ExtLong(it, defValue) })

class ExtLong(combineKV: CombineKV, private val defValue: Long) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Long {
        return combineKV.getLong(extKey.toString(), defValue)
    }

    operator fun set(extKey: Any, value: Long) {
        combineKV.putLong(extKey.toString(), value)
    }
}

class ExtNullableLongProperty(key: String) :
    ExtProperty<ExtNullableLong>(key, { ExtNullableLong(it) })

class ExtNullableLong(combineKV: CombineKV) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Long? {
        return combineKV.getLong(extKey.toString())
    }

    operator fun set(extKey: Any, value: Long?) {
        combineKV.putLong(extKey.toString(), value)
    }
}

//--------------------------------------------------------------------

class ExtDoubleProperty(key: String, defValue: Double) :
    ExtProperty<ExtDouble>(key, { ExtDouble(it, defValue) })

class ExtDouble(combineKV: CombineKV, private val defValue: Double) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Double {
        return combineKV.getDouble(extKey.toString(), defValue)
    }

    operator fun set(extKey: Any, value: Double) {
        combineKV.putDouble(extKey.toString(), value)
    }
}

class ExtNullableDoubleProperty(key: String) :
    ExtProperty<ExtNullableDouble>(key, { ExtNullableDouble(it) })

class ExtNullableDouble(combineKV: CombineKV) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Double? {
        return combineKV.getDouble(extKey.toString())
    }

    operator fun set(extKey: Any, value: Double?) {
        combineKV.putDouble(extKey.toString(), value)
    }
}

//--------------------------------------------------------------------

class ExtStringProperty(key: String, defValue: String) :
    ExtProperty<ExtString>(key, { ExtString(it, defValue) })

class ExtString(combineKV: CombineKV, private val defValue: String) : ExtKV(combineKV) {
    operator fun get(extKey: Any): String {
        return combineKV.getString(extKey.toString()) ?: defValue
    }

    operator fun set(extKey: Any, value: String) {
        combineKV.putString(extKey.toString(), value)
    }
}

class ExtNullableStringProperty(key: String) :
    ExtProperty<ExtNullableString>(key, { ExtNullableString(it) })

class ExtNullableString(combineKV: CombineKV) : ExtKV(combineKV) {
    operator fun get(extKey: Any): String? {
        return combineKV.getString(extKey.toString())
    }

    operator fun set(extKey: Any, value: String?) {
        combineKV.putString(extKey.toString(), value)
    }
}

//--------------------------------------------------------------------

class ExtArrayProperty(key: String, defValue: ByteArray) :
    ExtProperty<ExtArray>(key, { ExtArray(it, defValue) })

class ExtArray(combineKV: CombineKV, private val defValue: ByteArray) : ExtKV(combineKV) {
    operator fun get(extKey: Any): ByteArray {
        return combineKV.getArray(extKey.toString()) ?: defValue
    }

    operator fun set(extKey: Any, value: ByteArray) {
        combineKV.putArray(extKey.toString(), value)
    }
}

class ExtNullableArrayProperty(key: String) :
    ExtProperty<ExtNullableArray>(key, { ExtNullableArray(it) })

class ExtNullableArray(combineKV: CombineKV) : ExtKV(combineKV) {
    operator fun get(extKey: Any): ByteArray? {
        return combineKV.getArray(extKey.toString())
    }

    operator fun set(extKey: Any, value: ByteArray?) {
        combineKV.putArray(extKey.toString(), value)
    }
}

//--------------------------------------------------------------------

class ExtStringSetProperty(key: String, defValue: Set<String>) :
    ExtProperty<ExtSetString>(key, { ExtSetString(it, defValue) })

class ExtSetString(combineKV: CombineKV, private val defValue: Set<String>) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Set<String> {
        return combineKV.getStringSet(extKey.toString()) ?: defValue
    }

    operator fun set(extKey: Any, value: Set<String>) {
        combineKV.putStringSet(extKey.toString(), value)
    }
}

class ExtNullableStringSetProperty(key: String) :
    ExtProperty<ExtSetNullableString>(key, { ExtSetNullableString(it) })

class ExtSetNullableString(combineKV: CombineKV) : ExtKV(combineKV) {
    operator fun get(extKey: Any): Set<String>? {
        return combineKV.getStringSet(extKey.toString())
    }

    operator fun set(extKey: Any, value: Set<String>?) {
        combineKV.putStringSet(extKey.toString(), value)
    }
}

//--------------------------------------------------------------------

class ExtObjectProperty<T>(key: String, encoder: FastEncoder<T>, defValue: T) :
    ExtProperty<ExtObject<T>>(key, { ExtObject(it, encoder, defValue) })

class ExtObject<T>(
    combineKV: CombineKV,
    private val encoder: FastEncoder<T>,
    private val defValue: T
) : ExtKV(combineKV) {
    operator fun get(extKey: Any): T {
        return combineKV.getObject(extKey.toString()) ?: defValue
    }

    operator fun set(extKey: Any, value: T) {
        combineKV.putObject(extKey.toString(), value, encoder)
    }
}

class ExtNullableObjectProperty<T>(key: String, encoder: FastEncoder<T>) :
    ExtProperty<ExtNullableObject<T>>(key, { ExtNullableObject(it, encoder) })

class ExtNullableObject<T>(
    combineKV: CombineKV,
    private val encoder: FastEncoder<T>,
) : ExtKV(combineKV) {
    operator fun get(extKey: Any): T? {
        return combineKV.getObject(extKey.toString())
    }

    operator fun set(extKey: Any, value: T?) {
        combineKV.putObject(extKey.toString(), value, encoder)
    }
}

//--------------------------------------------------------------------

