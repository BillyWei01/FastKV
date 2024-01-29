package io.fastkv.fastkvdemo.fastkv.utils

import io.fastkv.interfaces.FastEncoder
import io.packable.PackDecoder
import io.packable.PackEncoder


object LongListEncoder : FastEncoder<List<Long>> {
    override fun tag(): String {
        return "LongList"
    }

    override fun encode(obj: List<Long>): ByteArray {
        return PackEncoder().putLongList(0, obj).bytes
    }

    override fun decode(bytes: ByteArray, offset: Int, length: Int): List<Long> {
        return ArrayList(PackDecoder.newInstance(bytes, offset, length).getLongList(0))
    }
}