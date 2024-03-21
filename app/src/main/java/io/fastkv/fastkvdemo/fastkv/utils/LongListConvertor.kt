package io.fastkv.fastkvdemo.fastkv.utils

import io.fastkv.fastkvdemo.fastkv.kvdelegate.ObjectConvertor
import io.packable.PackDecoder
import io.packable.PackEncoder

object LongListConvertor : ObjectConvertor<List<Long>> {
    override fun encode(obj: List<Long>): ByteArray {
        return PackEncoder().putLongList(0, obj).bytes
    }

    override fun decode(bytes: ByteArray): List<Long> {
        return ArrayList(PackDecoder.newInstance(bytes).getLongList(0))
    }
}