package io.fastkv.fastkvdemo.fastkv.kvbase

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.fastkv.kvdelegate.KVData
import io.fastkv.fastkvdemo.manager.PathManager

open class GlobalKV(name: String) : KVData() {
    override val kv: FastKV by lazy {
        buildKV(PathManager.fastKVDir, name)
    }
}