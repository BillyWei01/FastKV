package io.fastkv.fastkvdemo.fastkv

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.manager.PathManager

open class GlobalStorage(name: String) : KVData() {
    override val kv: FastKV by lazy {
        buildKV(PathManager.fastKVDir, name)
    }
}