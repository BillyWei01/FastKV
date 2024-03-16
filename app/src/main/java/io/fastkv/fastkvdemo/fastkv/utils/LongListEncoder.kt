package io.fastkv.fastkvdemo.fastkv.utils

import io.fastkv.fastkvdemo.fastkv.kvdelegate.ObjectEncoder
import io.packable.PackDecoder
import io.packable.PackEncoder

object LongListEncoder : ObjectEncoder<List<Long>> {
    override fun encode(obj: List<Long>): ByteArray {
        return PackEncoder().putLongList(0, obj).bytes
    }

    override fun decode(data: ByteArray): List<Long> {
        return ArrayList(PackDecoder.newInstance(data).getLongList(0))
    }
}