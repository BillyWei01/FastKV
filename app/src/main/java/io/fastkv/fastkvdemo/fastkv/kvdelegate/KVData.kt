package io.fastkv.fastkvdemo.fastkv.kvdelegate

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.fastkv.cipher.CipherManager
import io.fastkv.interfaces.FastCipher
import io.fastkv.interfaces.FastEncoder

abstract class KVData {
    abstract val kv: FastKV

    protected open fun encoders(): Array<FastEncoder<*>>? {
        return null
    }

    protected open fun cipher(): FastCipher? {
        return CipherManager.defaultCipher
    }

    protected fun buildKV(path: String, name: String): FastKV {
        return FastKV.Builder(path, name)
            .encoder(encoders())
            .cipher(cipher())
            .build()
    }


    // ------------定义简短的属性委托API，以方便使用--------------

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

    protected fun <T> obj(key: String, encoder: FastEncoder<T>, defValue: T? = null) =
        ObjectProperty(key, encoder, defValue)

    protected fun <T> stringEnum(key: String, converter: StringEnumConverter<T>) =
        StringEnumProperty(key, converter)

    protected fun <T> intEnum(key: String, converter: IntEnumConverter<T>) =
        IntEnumProperty(key, converter)

    protected fun combineKey(key: String) = CombineKeyProperty(key)
    protected fun string2String(key: String) = StringToStringProperty(key)
    protected fun string2Set(key: String) = StringToSetProperty(key)
    protected fun string2Int(key: String) = StringToIntProperty(key)
    protected fun string2Boolean(key: String) = StringToBooleanProperty(key)
    protected fun int2Boolean(key: String) = IntToBooleanProperty(key)

    companion object {
        private val EMPTY_ARRAY = ByteArray(0)
    }
}





