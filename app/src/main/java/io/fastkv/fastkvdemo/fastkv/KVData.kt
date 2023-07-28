package io.fastkv.fastkvdemo.fastkv

import io.fastkv.interfaces.FastCipher
import io.fastkv.FastKV
import io.fastkv.interfaces.FastEncoder
import kotlin.properties.ReadOnlyProperty
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

abstract class KVData {
    // Make kv to be protected or public/internal ?
    protected abstract val kv: FastKV

    protected open fun encoders(): Array<FastEncoder<*>>? {
        return null
    }

    protected open fun cipher(): FastCipher? {
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
    protected fun array(key: String, defValue: ByteArray = EMPTY_ARRAY) = ArrayProperty(key, defValue)
    protected fun stringSet(key: String, defValue: Set<String>? = null) = StringSetProperty(key, defValue)
    protected fun <T> obj(key: String, encoder: FastEncoder<T>) = ObjectProperty(key, encoder)

    /**
     * Note:
     * This property only supports 'put', 'get', 'containsKey', and 'remove'.
     * Calling other method will throw UnsupportedOperationException.
     */
    protected fun map(key: String) = MapProperty(key)

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

    class ObjectProperty<T>(private val key: String, private val encoder: FastEncoder<T>) :
        ReadWriteProperty<KVData, T?> {
        override fun getValue(thisRef: KVData, property: KProperty<*>): T? {
            return thisRef.kv.getObject(key)
        }

        override fun setValue(thisRef: KVData, property: KProperty<*>, value: T?) {
            thisRef.kv.putObject(key, value, encoder)
        }
    }

    class MapProperty(private val key: String) : ReadOnlyProperty<KVData, MutableMap<String, String>> {
        @Volatile
        private var proxyMap: ProxyMap? = null
        private fun getMap(kvData: KVData): ProxyMap {
            if (proxyMap == null) {
                synchronized(this) {
                    if (proxyMap == null) {
                        proxyMap = ProxyMap(kvData, key)
                    }
                }
            }
            return proxyMap!!
        }

        override fun getValue(thisRef: KVData, property: KProperty<*>): MutableMap<String, String> {
            return getMap(thisRef)
        }
    }

    companion object {
        val EMPTY_ARRAY = ByteArray(0)

        class ProxyMap(private val kvData: KVData, private val preKey: String) :
            java.util.AbstractMap<String, String>() {
            override val entries: MutableSet<MutableMap.MutableEntry<String, String>>
                get() = throw UnsupportedOperationException()

            private fun combineKey(key: String): String {
                return "$preKey&$key"
            }

            override fun get(key: String?): String? {
                return if (key == null) null else kvData.kv.getString(combineKey(key), null)
            }

            override fun put(key: String?, value: String?): String? {
                if (key == null) return null
                kvData.kv.putString(combineKey(key), value)
                return null
            }

            override fun containsKey(key: String?): Boolean {
                return if (key == null) false else kvData.kv.contains(combineKey(key))
            }

            override fun remove(key: String?): String? {
                if (key == null) return null
                kvData.kv.remove(combineKey(key))
                return null
            }
        }
    }
}


