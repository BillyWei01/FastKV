package io.fastkv.fastkvdemo.fastkv.kvdelegate

import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty


//------------------------枚举类型属性委托的实现-------------------------

class StringEnumProperty<T>(
    private val key: String,
    private val converter: StringEnumConverter<T>
) : ReadWriteProperty<KVData, T> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): T {
        return converter.stringToType(thisRef.kv.getString(key))
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: T) {
        thisRef.kv.putString(key, converter.typeToString(value))
    }
}

interface StringEnumConverter<T> {
    fun stringToType(str: String?): T
    fun typeToString(type: T): String
}

//----------------------------------------------------

class IntEnumProperty<T>(
    private val key: String,
    private val converter: IntEnumConverter<T>
) : ReadWriteProperty<KVData, T> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): T {
        return converter.intToType(thisRef.kv.getInt(key))
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: T) {
        thisRef.kv.putInt(key, converter.typeToInt(value))
    }
}

interface IntEnumConverter<T> {
    fun intToType(value: Int): T
    fun typeToInt(type: T): Int
}