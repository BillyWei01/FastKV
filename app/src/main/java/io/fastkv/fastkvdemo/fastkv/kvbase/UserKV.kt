package io.fastkv.fastkvdemo.fastkv.kvbase

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.fastkv.cipher.CipherManager
import io.fastkv.fastkvdemo.fastkv.kvdelegate.KVData
import io.fastkv.fastkvdemo.manager.PathManager

/**
 * 用户数据存储
 *
 * 需要同时区分环境和uid。
 */
abstract class UserKV(
    private val name: String,
    private val userId: Long
) : KVData() {
    override val kv: FastKV by lazy {
        val dir = "${userId}_${AppContext.env.tag}"
        val path = PathManager.fastKVDir + "/user/" + dir
        FastKV.Builder(path, name)
            .encoder(encoders())
            .cipher(CipherManager.defaultCipher)
            .build()
    }

    /*
        //  如果release包需要隐藏uid，可以做个对dir做md5运算
        val dir = "${userId}_${AppContext.env.tag}".let {
            if (AppContext.debug) it else Utils.getMD5(it.toByteArray())
        }
    */
}
