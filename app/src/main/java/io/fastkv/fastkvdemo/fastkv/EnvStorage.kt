package io.fastkv.fastkvdemo.fastkv

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.manager.PathManager

/**
 * 根据服务器环境（正式服/测试服，或者线上环境/开发环境）,区分目录
 */
open class EnvStorage(private val name: String) : KVData() {
    override val kv: FastKV by lazy {
        val path = PathManager.fastKVDir +"/"+AppContext.env.tag
        buildKV(path, name)
    }
}
