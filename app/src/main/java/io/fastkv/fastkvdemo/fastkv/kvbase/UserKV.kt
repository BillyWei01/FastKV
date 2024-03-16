package io.fastkv.fastkvdemo.fastkv.kvbase

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.fastkv.cipher.CipherManager
import io.fastkv.fastkvdemo.fastkv.kvdelegate.KVData
import io.fastkv.fastkvdemo.fastkv.kvdelegate.KVStore
import io.fastkv.fastkvdemo.fastkv.utils.FastKVStore
import io.fastkv.fastkvdemo.manager.PathManager
import io.fastkv.fastkvdemo.util.Utils

/**
 * 用户数据
 *
 * 需要同时区分 “环境“ 和 ”用户“。
 */
abstract class UserKV(
    private val name: String,
    private val userId: Long
) : KVData() {
    override val kv: KVStore by lazy {
        val dir = "${userId}_${AppContext.env.tag}"
        val finalDir = if (AppContext.debug) {
            dir
        } else {
            // 如果是release包，可以对路径名做个md5，以便匿藏uid等信息
            Utils.getMD5(dir.toByteArray())
        }
        val path = PathManager.fastKVDir + "/user/" + finalDir
        FastKVStore(
            FastKV.Builder(path, name)
                .cipher(CipherManager.defaultCipher)
                .build()
        )
    }
}
