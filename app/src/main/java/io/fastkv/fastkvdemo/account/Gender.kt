package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.fastkv.IntEnumConverter

enum class Gender(private val value: Int) {
    UNKNOWN(0),
    MALE(1),
    FEMALE(2);

    companion object {
        val CONVERTER = object : IntEnumConverter<Gender> {
            override fun intToType(value: Int): Gender {
                return when (value) {
                    1 -> MALE
                    2 -> FEMALE
                    else -> UNKNOWN
                }
            }

            override fun typeToInt(type: Gender): Int {
                return type.value
            }
        }
    }
}