package io.fastkv.fastkvdemo.fastkv.kvdelegate

interface ObjectEncoder<T> {
    fun encode(obj: T): ByteArray

    fun decode(data: ByteArray): T
}

interface NullableObjectEncoder<T> {
    fun encode(obj: T?): ByteArray?

    fun decode(data: ByteArray?): T?
}