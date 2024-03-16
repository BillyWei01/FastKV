package io.fastkv.fastkvdemo.fastkv.kvdelegate

import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty


//------------------------基础类型属性委托的实现-------------------------

class BooleanProperty(private val key: String, private val defValue: Boolean) :
    ReadWriteProperty<KVData, Boolean> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Boolean {
        return thisRef.kv.getBoolean(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Boolean) {
        thisRef.kv.putBoolean(key, value)
    }
}

class NullableBooleanProperty(private val key: String) :
    ReadWriteProperty<KVData, Boolean?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Boolean? {
        return thisRef.kv.getBoolean(key)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Boolean?) {
        thisRef.kv.putBoolean(key, value)
    }
}

//--------------------------------------------------------------------

class IntProperty(private val key: String, private val defValue: Int) :
    ReadWriteProperty<KVData, Int> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Int {
        return thisRef.kv.getInt(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Int) {
        thisRef.kv.putInt(key, value)
    }
}

class NullableIntProperty(private val key: String) :
    ReadWriteProperty<KVData, Int?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Int? {
        return thisRef.kv.getInt(key)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Int?) {
        thisRef.kv.putInt(key, value)
    }
}

//--------------------------------------------------------------------

class FloatProperty(private val key: String, private val defValue: Float) :
    ReadWriteProperty<KVData, Float> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Float {
        return thisRef.kv.getFloat(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Float) {
        thisRef.kv.putFloat(key, value)
    }
}

class NullableFloatProperty(private val key: String) :
    ReadWriteProperty<KVData, Float?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Float? {
        return thisRef.kv.getFloat(key)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Float?) {
        thisRef.kv.putFloat(key, value)
    }
}

class LongProperty(private val key: String, private val defValue: Long) :
    ReadWriteProperty<KVData, Long> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Long {
        return thisRef.kv.getLong(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Long) {
        thisRef.kv.putLong(key, value)
    }
}

class NullableLongProperty(private val key: String) :
    ReadWriteProperty<KVData, Long?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Long? {
        return thisRef.kv.getLong(key)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Long?) {
        thisRef.kv.putLong(key, value)
    }
}

//--------------------------------------------------------------------

class DoubleProperty(private val key: String, private val defValue: Double) :
    ReadWriteProperty<KVData, Double> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Double {
        return thisRef.kv.getDouble(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Double) {
        thisRef.kv.putDouble(key, value)
    }
}

class NullableDoubleProperty(private val key: String) :
    ReadWriteProperty<KVData, Double?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Double? {
        return thisRef.kv.getDouble(key)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Double?) {
        thisRef.kv.putDouble(key, value)
    }
}

//--------------------------------------------------------------------

class StringProperty(private val key: String, private val defValue: String) :
    ReadWriteProperty<KVData, String> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): String {
        return thisRef.kv.getString(key) ?: defValue
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: String) {
        thisRef.kv.putString(key, value)
    }
}

class NullableStringProperty(private val key: String) :
    ReadWriteProperty<KVData, String?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): String? {
        return thisRef.kv.getString(key)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: String?) {
        thisRef.kv.putString(key, value)
    }
}

//--------------------------------------------------------------------

class ArrayProperty(private val key: String, private val defValue: ByteArray) :
    ReadWriteProperty<KVData, ByteArray> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): ByteArray {
        return thisRef.kv.getArray(key) ?: defValue
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: ByteArray) {
        thisRef.kv.putArray(key, value)
    }
}

class NullableArrayProperty(private val key: String) :
    ReadWriteProperty<KVData, ByteArray?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): ByteArray? {
        return thisRef.kv.getArray(key)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: ByteArray?) {
        thisRef.kv.putArray(key, value)
    }
}

//--------------------------------------------------------------------

class StringSetProperty(private val key: String, private val defValue: Set<String>) :
    ReadWriteProperty<KVData, Set<String>> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Set<String> {
        return thisRef.kv.getStringSet(key) ?: defValue
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Set<String>) {
        thisRef.kv.putStringSet(key, value)
    }
}

class NullableStringSetProperty(private val key: String) :
    ReadWriteProperty<KVData, Set<String>?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Set<String>? {
        return thisRef.kv.getStringSet(key)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Set<String>?) {
        thisRef.kv.putStringSet(key, value)
    }
}

//--------------------------------------------------------------------

class ObjectProperty<T>(
    private val key: String,
    private val encoder: ObjectEncoder<T>,
    private val defValue: T
) : ReadWriteProperty<KVData, T> {
    private var instance: T? = null

    @Synchronized
    override fun getValue(thisRef: KVData, property: KProperty<*>): T {
        return instance ?: thisRef.kv.getObject(key, encoder, defValue).also { instance = it }
    }

    @Synchronized
    override fun setValue(thisRef: KVData, property: KProperty<*>, value: T) {
        instance = value
        thisRef.kv.putObject(key, value, encoder)
    }
}

class NullableObjectSetProperty<T>(
    private val key: String,
    private val encoder: NullableObjectEncoder<T>
) :
    ReadWriteProperty<KVData, T?> {
    private var instance: T? = null

    @Synchronized
    override fun getValue(thisRef: KVData, property: KProperty<*>): T? {
        return instance ?: thisRef.kv.getNullableObject(key, encoder)?.also { instance = it }
    }

    @Synchronized
    override fun setValue(thisRef: KVData, property: KProperty<*>, value: T?) {
        instance = value
        thisRef.kv.putNullableObject(key, value, encoder)
    }
}
