package io.fastkv.fastkvdemo.fastkv.kvbase

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.fastkv.kvdelegate.KVData
import io.fastkv.fastkvdemo.manager.PathManager

open class GlobalKV(name: String) : KVData() {
    override val kv: FastKV by lazy {
        FastKV.Builder(PathManager.fastKVDir, name).encoder(encoders()).build()
    }
}