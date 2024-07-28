package io.fastkv.fastkvdemo.fastkv.kvdelegate

interface ObjectConvertor<T> {
    fun encode(obj: T): ByteArray

    fun decode(bytes: ByteArray): T
}
