package io.fastkv.fastkvdemo.fastkv.kvdelegate

import io.fastkv.interfaces.FastEncoder

/**
 * KVData封装了两类属性委托：
 *
 * 1. 基础类型
 * 基础类型包括 [boolean, int, float, long, double, String, byte[], Set<String>, Object] 等类型。
 * 其中，Set<String> 本可以通过Object类型囊括，但因为Set<String>是 FastKV 内置支持的类型，
 * 所以这里我们直接内置支持了。
 *
 * 2. 扩展key的基础类型
 * 基础类型的委托，定义属性时需传入常量的`key`，通过委托所访问到的是`key`对应的`value`；
 * 而开发中有时候需要【常量+变量】的key，基础类型的委托无法实现。
 * 为此，方案中实现了一个[CombineKV]类，通过[CombineKV]也可以访问`value`, 但是需要额外多传一个`extKey`。
 * [CombineKV]通过组合 `key+extKey` 实现通过两级key来访问`value`的效果。<br>
 * 此外，方案基于[CombineKV]封装了各种基础类型的[ExtKV]，用于简化API，以及约束所访问`value`的类型。
 *
 * 每一种类型都分别封装了可空和非空两种委托实现。
 */
abstract class KVData {
    /**
     * 由子类提供KV实例
     */
    abstract val kv: KVStore

    // 基础类型
    protected fun boolean(key: String, defValue: Boolean = false) = BooleanProperty(key, defValue)
    protected fun int(key: String, defValue: Int = 0) = IntProperty(key, defValue)
    protected fun float(key: String, defValue: Float = 0f) = FloatProperty(key, defValue)
    protected fun long(key: String, defValue: Long = 0L) = LongProperty(key, defValue)
    protected fun double(key: String, defValue: Double = 0.0) = DoubleProperty(key, defValue)
    protected fun string(key: String, defValue: String = "") = StringProperty(key, defValue)
    protected fun array(key: String, defValue: ByteArray = EMPTY_ARRAY) = ArrayProperty(key, defValue)
    protected fun stringSet(key: String, defValue: Set<String> = emptySet()) = StringSetProperty(key, defValue)
    protected fun <T> obj(key: String, encoder: ObjectEncoder<T>, defValue: T) = ObjectProperty(key, encoder, defValue)

    protected fun nullableBoolean(key: String) = NullableBooleanProperty(key)
    protected fun nullableInt(key: String) = NullableIntProperty(key)
    protected fun nullableFloat(key: String) = NullableFloatProperty(key)
    protected fun nullableLong(key: String) = NullableLongProperty(key)
    protected fun nullableDouble(key: String) = NullableDoubleProperty(key)
    protected fun nullableString(key: String) = NullableStringProperty(key)
    protected fun nullableArray(key: String) = NullableArrayProperty(key)
    protected fun nullableStringSet(key: String) = NullableStringSetProperty(key)
    protected fun <T> nullableObj(key: String, encoder: NullableObjectEncoder<T>) = NullableObjectSetProperty(key, encoder)

    // 扩展key的基础类型
    protected fun extBoolean(key: String, defValue: Boolean = false) = ExtBooleanProperty(key, defValue)
    protected fun extInt(key: String, defValue: Int = 0) = ExtIntProperty(key, defValue)
    protected fun extFloat(key: String, defValue: Float = 0f) = ExtFloatProperty(key, defValue)
    protected fun extLong(key: String, defValue: Long = 0L) = ExtLongProperty(key, defValue)
    protected fun extDouble(key: String, defValue: Double = 0.0) = ExtDoubleProperty(key, defValue)
    protected fun extString(key: String, defValue: String = "") = ExtStringProperty(key, defValue)
    protected fun extArray(key: String, defValue: ByteArray = EMPTY_ARRAY) = ExtArrayProperty(key, defValue)
    protected fun extStringSet(key: String, defValue: Set<String> = emptySet()) = ExtStringSetProperty(key, defValue)
    protected fun <T> extObj(key: String, encoder: ObjectEncoder<T>, defValue: T) = ExtObjectProperty(key, encoder, defValue)

    protected fun extNullableBoolean(key: String) = ExtNullableBooleanProperty(key)
    protected fun extNullableInt(key: String) = ExtNullableIntProperty(key)
    protected fun extNullableFloat(key: String) = ExtNullableFloatProperty(key)
    protected fun extNullableLong(key: String) = ExtNullableLongProperty(key)
    protected fun extNullableDouble(key: String) = ExtNullableDoubleProperty(key)
    protected fun extNullableString(key: String) = ExtNullableStringProperty(key)
    protected fun extNullableArray(key: String) = ExtNullableArrayProperty(key)
    protected fun extNullableStringSet(key: String) = ExtNullableStringSetProperty(key)
    protected fun <T> extNullableObj(key: String, encoder: NullableObjectEncoder<T>) = ExtNullableObjectProperty(key, encoder)

    /**
     * [CombineKV] 属性委托
     *
     * 扩展key的基础类型，每种定义都对应了明确的类型。
     * 而 [CombineKV] 不约束set/get的类型，可以读写多种类型。
     */
    //
    protected fun combineKV(key: String) = CombineKVProperty(key)

    companion object {
        private val EMPTY_ARRAY = ByteArray(0)
    }
}

