package io.fastkv.fastkvdemo.util

import java.security.MessageDigest

object Digest {
    fun md5(msg: ByteArray): String {
        return kotlin.runCatching {
            val bytes = MessageDigest.getInstance("MD5").digest(msg)
            Utils.bytes2Hex(bytes)
        }.getOrDefault(Utils.bytes2Hex(msg))
    }
}