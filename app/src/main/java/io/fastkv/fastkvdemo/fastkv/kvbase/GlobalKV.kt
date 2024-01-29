package io.fastkv.fastkvdemo.fastkv.kvbase

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.fastkv.kvdelegate.KVData
import io.fastkv.fastkvdemo.fastkv.kvdelegate.KVStore
import io.fastkv.fastkvdemo.fastkv.utils.FastKVStore
import io.fastkv.fastkvdemo.manager.PathManager

/**
 * 全局数据
 *
 * 切换环境和用户，不影响GlobalKV所访问的数据实例。
 */
open class GlobalKV(name: String) : KVData() {
    override val kv: KVStore by lazy {
        FastKVStore(
            FastKV.Builder(PathManager.fastKVDir, name)
                .encoder(encoders())
                .build()
        )
    }
}