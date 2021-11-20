package io.fastkv.fastkvdemo.fastkv

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.manager.PathManager
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

open class KVData(name: String) {
    val kv: FastKV by lazy {
        FastKV.Builder(PathManager.fastKVDir, name).encoder(encoders()).build()
    }

    protected open fun encoders(): Array<FastKV.Encoder<*>>? {
        return null
    }

    protected fun boolean(key: String, defValue: Boolean = false) = BooleanProperty(key, defValue)
    protected fun int(key: String, defValue: Int = 0) = IntProperty(key, defValue)
    protected fun float(key: String, defValue: Float = 0f) = FloatProperty(key, defValue)
    protected fun long(key: String, defValue: Long = 0L) = LongProperty(key, defValue)
    protected fun double(key: String, defValue: Double = 0.0) = DoubleProperty(key, defValue)
    protected fun string(key: String, defValue: String = "") = StringProperty(key, defValue)
    protected fun array(key: String, defValue: ByteArray = EMPTY_ARRAY) = ArrayProperty(key, defValue)
    protected fun stringSet(key: String, defValue: Set<String>? = null) = StringSetProperty(key, defValue)
    protected fun <T> obj(key: String, encoder: FastKV.Encoder<T>) = ObjectProperty(key, encoder)

    companion object {
        val EMPTY_ARRAY = ByteArray(0)
    }
}

class BooleanProperty(private val key: String, private val defValue: Boolean) :
    ReadWriteProperty<KVData, Boolean> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Boolean {
        return thisRef.kv.getBoolean(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Boolean) {
        thisRef.kv.putBoolean(key, value)
    }
}

class IntProperty(private val key: String, private val defValue: Int) :
    ReadWriteProperty<KVData, Int> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Int {
        return thisRef.kv.getInt(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Int) {
        thisRef.kv.putInt(key, value)
    }
}

class FloatProperty(private val key: String, private val defValue: Float) :
    ReadWriteProperty<KVData, Float> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Float {
        return thisRef.kv.getFloat(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Float) {
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

class DoubleProperty(private val key: String, private val defValue: Double) :
    ReadWriteProperty<KVData, Double> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Double {
        return thisRef.kv.getDouble(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Double) {
        thisRef.kv.putDouble(key, value)
    }
}

class StringProperty(private val key: String, private val defValue: String) :
    ReadWriteProperty<KVData, String> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): String {
        return thisRef.kv.getString(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: String) {
        thisRef.kv.putString(key, value)
    }
}

class ArrayProperty(private val key: String, private val defValue: ByteArray) :
    ReadWriteProperty<KVData, ByteArray> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): ByteArray {
        return thisRef.kv.getArray(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: ByteArray) {
        thisRef.kv.putArray(key, value)
    }
}

class StringSetProperty(private val key: String, private val defValue: Set<String>?) :
    ReadWriteProperty<KVData, Set<String>?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Set<String>? {
        return thisRef.kv.getStringSet(key) ?: defValue
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Set<String>?) {
        thisRef.kv.putStringSet(key, value)
    }
}

class ObjectProperty<T>(private val key: String, private val encoder: FastKV.Encoder<T>) :
    ReadWriteProperty<KVData, T?> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): T? {
        return thisRef.kv.getObject(key)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: T?) {
        thisRef.kv.putObject(key, value, encoder)
    }
}

