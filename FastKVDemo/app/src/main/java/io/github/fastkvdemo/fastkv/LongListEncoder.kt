package io.github.fastkvdemo.fastkv

import io.fastkv.FastKV
import io.packable.PackDecoder
import io.packable.PackEncoder

object LongListEncoder : FastKV.Encoder<List<Long>> {
    override fun tag(): String {
        return "LongList"
    }

    override fun encode(obj: List<Long>): ByteArray {
        return PackEncoder().putLongList(0, obj).bytes
    }

    override fun decode(bytes: ByteArray, offset: Int, length: Int): List<Long> {
        val decoder = PackDecoder.newInstance(bytes, offset, length)
        val list = decoder.getLongList(0)
        decoder.recycle()
        return list ?: listOf()
    }
}