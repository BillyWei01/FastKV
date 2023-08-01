package io.fastkv.fastkvdemo.base

import io.fastkv.fastkvdemo.fastkv.StringEnumConverter


/**
 * Server Environment
 *
 * 命名为ServerEnvironment又太长，
 * 命名为Environment又太多重名的类，
 * 所以命名为简化的Env好了。
 */
enum class Env(val tag: String) {
    // 线上环境（正式服）
    ONLINE(""),

    // 开发环境（测试服)
    DEVELOP("develop");

    companion object {
        val CONVERTER = object : StringEnumConverter<Env> {
            override fun stringToType(str: String?): Env {
               return if (str == DEVELOP.tag) DEVELOP else ONLINE
            }

            override fun typeToString(type: Env): String {
               return type.tag
            }
        }
    }
}