package io.fastkv.fastkvdemo.account

import io.fastkv.interfaces.FastEncoder
import java.nio.ByteBuffer


enum class Gender(private val value: Int) {
    UNKNOWN(0),
    MALE(1),
    FEMALE(2);

    companion object {
        fun intToType(value: Int): Gender {
            return when (value) {
                1 -> MALE
                2 -> FEMALE
                else -> UNKNOWN
            }
        }

        val CONVERTER = object : FastEncoder<Gender> {
            override fun tag(): String {
                return "Gender"
            }

            override fun decode(bytes: ByteArray, offset: Int, length: Int): Gender {
                return intToType(ByteBuffer.wrap(bytes, offset, length).int)
            }

            override fun encode(obj: Gender): ByteArray {
                return ByteBuffer.allocate(4).putInt(obj.value).array()
            }
        }
    }
}