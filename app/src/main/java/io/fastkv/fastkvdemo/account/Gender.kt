package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.fastkv.kvdelegate.ObjectEncoder
import java.nio.ByteBuffer


enum class Gender(private val value: Int) {
    UNKNOWN(0),
    MALE(1),
    FEMALE(2);

    companion object {
        fun parse(value: Int): Gender {
            return when (value) {
                1 -> MALE
                2 -> FEMALE
                else -> UNKNOWN
            }
        }

        val CONVERTER = object : ObjectEncoder<Gender> {
            override fun encode(obj: Gender): ByteArray {
                return ByteBuffer.allocate(4).putInt(obj.value).array()
            }

            override fun decode(data: ByteArray): Gender {
                return parse(ByteBuffer.wrap(data).int)
            }
        }
    }
}