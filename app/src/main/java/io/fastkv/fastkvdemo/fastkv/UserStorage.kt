package io.fastkv.fastkvdemo.fastkv

import android.util.LongSparseArray
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.manager.PathManager

/**
 * 用户数据存储
 *
 * 如果不同环境下的UID绝不会相等（比如测试环境uid从1万开始，正式环境uid从1亿开始，测试账号不回到达1亿），
 * 则不需要再根据env区分。
 * 否则，需要同时区分环境和uid
 *
 * 包用用户数据，在切换用户时需要注意不要让数据写错到另外用户的空间。
 */
abstract class UserStorage(
    private val name: String,
    private val userId: Long
) : KVData() {
    companion object {
        private val kvCache = LongSparseArray<HashMap<String, FastKV>>()
    }

    override val kv: FastKV
        get() {
            synchronized(kvCache) {
                val uid = if (userId != 0L) userId else AppContext.uid
                var group = kvCache.get(uid)
                if (group == null) {
                    group = HashMap()
                    kvCache.put(uid, group)
                }
                return group.getOrPut(name) {
                    val path = PathManager.fastKVDir + "/user/" + uid
                    buildKV(path, name)
                }
            }
        }

    protected fun closeFastKV(uid: Long) {
        synchronized(kvCache) {
            kvCache[uid]?.let { group ->
                group.remove(name)?.close()
            }
        }
    }


    /*
    // 区分环境同时加密uid
    private fun encrypt(uid: Long): String {
        val tag = AppContext.env.tag
        val encryptUid = CipherManager.numberCipher.encryptLong(uid)
        val uidStr = Utils.bytes2Hex(NumberCipher.long2Bytes(encryptUid))
        return if (tag.isEmpty()) uidStr else "$uidStr-$tag"
    }
    */
}