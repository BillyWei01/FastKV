package io.fastkv.fastkvdemo.fastkv

import io.fastkv.interfaces.FastCipher
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.fastkv.cipher.CipherManager
import io.fastkv.interfaces.FastEncoder
import kotlin.properties.ReadOnlyProperty
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

abstract class KVData {
    protected abstract val kv: FastKV

    protected open fun encoders(): Array<FastEncoder<*>>? {
        return null
    }

    protected open fun cipher(): FastCipher? {
        // return CipherManager.defaultCipher // 对所有FastKV加密
        return null
    }

    protected fun buildKV(path: String, name: String): FastKV {
        return FastKV.Builder(path, name)
            .encoder(encoders())
            .cipher(cipher())
            .build()
    }

    fun contains(key: String): Boolean {
        return kv.contains(key)
    }

    fun clear() {
        kv.clear()
    }

    protected fun boolean(key: String, defValue: Boolean = false) = BooleanProperty(key, defValue)
    protected fun int(key: String, defValue: Int = 0) = IntProperty(key, defValue)
    protected fun float(key: String, defValue: Float = 0f) = FloatProperty(key, defValue)
    protected fun long(key: String, defValue: Long = 0L) = LongProperty(key, defValue)
    protected fun double(key: String, defValue: Double = 0.0) = DoubleProperty(key, defValue)
    protected fun string(key: String, defValue: String = "") = StringProperty(key, defValue)
    protected fun array(key: String, defValue: ByteArray = EMPTY_ARRAY) =
        ArrayProperty(key, defValue)

    protected fun stringSet(key: String, defValue: Set<String>? = null) =
        StringSetProperty(key, defValue)

    protected fun <T> obj(key: String, encoder: FastEncoder<T>) = ObjectProperty(key, encoder)

    protected fun <T> stringEnum(key: String, converter: StringEnumConverter<T>) =
        StringEnumProperty(key, converter)

    protected fun <T> intEnum(key: String, converter: IntEnumConverter<T>) =
        IntEnumProperty(key, converter)

    protected fun combineKey(key: String) = CombineKeyProperty(key)

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
            return thisRef.kv.getString(key, null) ?: defValue
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

    class ObjectProperty<T>(private val key: String, private val encoder: FastEncoder<T>) :
        ReadWriteProperty<KVData, T?> {
        override fun getValue(thisRef: KVData, property: KProperty<*>): T? {
            return thisRef.kv.getObject(key)
        }

        override fun setValue(thisRef: KVData, property: KProperty<*>, value: T?) {
            thisRef.kv.putObject(key, value, encoder)
        }
    }

    class StringEnumProperty<T>(
        private val key: String,
        private val converter: StringEnumConverter<T>
    ) :
        ReadWriteProperty<KVData, T> {
        override fun getValue(thisRef: KVData, property: KProperty<*>): T {
            return converter.stringToType(thisRef.kv.getString(key))
        }

        override fun setValue(thisRef: KVData, property: KProperty<*>, value: T) {
            thisRef.kv.putString(key, converter.typeToString(value))
        }
    }

    class IntEnumProperty<T>(
        private val key: String,
        private val converter: IntEnumConverter<T>
    ) :
        ReadWriteProperty<KVData, T> {
        override fun getValue(thisRef: KVData, property: KProperty<*>): T {
            return converter.intToType(thisRef.kv.getInt(key))
        }

        override fun setValue(thisRef: KVData, property: KProperty<*>, value: T) {
            thisRef.kv.putInt(key, converter.typeToInt(value))
        }
    }

    inner class CombineKeyProperty(preKey: String) : ReadOnlyProperty<KVData, CombineKV> {
        private var combineKV = CombineKV(preKey)
        override fun getValue(thisRef: KVData, property: KProperty<*>): CombineKV {
            return combineKV
        }
    }

    inner class CombineKV(private val preKey: String) {
        private fun combineKey(key: String): String {
            return "$preKey&$key"
        }

        fun containsKey(key: String): Boolean {
            return kv.contains(combineKey(key))
        }

        fun remove(key: String) {
            kv.remove(combineKey(key))
        }

        fun putBoolean(key: String, value: Boolean) {
            kv.putBoolean(combineKey(key), value)
        }

        fun getBoolean(key: String, defValue: Boolean = false): Boolean {
            return kv.getBoolean(combineKey(key), defValue)
        }

        fun putInt(key: String, value: Int) {
            kv.putInt(combineKey(key), value)
        }

        fun getInt(key: String, defValue: Int = 0): Int {
            return kv.getInt(combineKey(key), defValue)
        }

        fun putFloat(key: String, value: Float) {
            kv.putFloat(combineKey(key), value)
        }

        fun getFloat(key: String, defValue: Float = 0f): Float {
            return kv.getFloat(combineKey(key), defValue)
        }

        fun putLong(key: String, value: Long) {
            kv.putLong(combineKey(key), value)
        }

        fun getLong(key: String, defValue: Long = 0L): Long {
            return kv.getLong(combineKey(key), defValue)
        }

        fun putDouble(key: String, value: Double) {
            kv.putDouble(combineKey(key), value)
        }

        fun getDouble(key: String, defValue: Double = 0.0): Double {
            return kv.getDouble(combineKey(key), defValue)
        }

        fun putString(key: String, value: String) {
            kv.putString(combineKey(key), value)
        }

        fun getString(key: String, defValue: String = ""): String {
            return kv.getString(combineKey(key), null) ?: defValue
        }

        fun putArray(key: String, value: ByteArray) {
            kv.putArray(combineKey(key), value)
        }

        fun getArray(key: String, defValue: ByteArray = EMPTY_ARRAY): ByteArray {
            return kv.getArray(combineKey(key), defValue)
        }

        fun <T> putObject(key: String, value: T, encoder: FastEncoder<T>) {
            kv.putObject(combineKey(key), value, encoder)
        }

        fun <T> getObject(key: String): T? {
            return kv.getObject(combineKey(key))
        }
    }

    companion object {
        val EMPTY_ARRAY = ByteArray(0)
    }
}

interface StringEnumConverter<T> {
    fun stringToType(str: String?): T
    fun typeToString(type: T): String
}

interface IntEnumConverter<T> {
    fun intToType(value: Int): T
    fun typeToInt(type: T): Int
}




