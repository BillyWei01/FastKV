package io.fastkv.fastkvdemo.fastkv

import android.util.ArrayMap
import android.util.LongSparseArray
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.manager.PathManager

/**
 * 用户数据存储
 *
 * 需要同时区分环境和uid。
 *
 * 在切换用户时需要注意不要让数据写错到另外用户的空间。
 * 这里我们用userId来控制所访问的用户空间。
 * 当 userId==0，所访问的用户空间根据当前登录用户的uid来决定；
 * 当 userId!=0，所访问的用户空间为userId指定的用户空间。
 *
 * 设计这一点，主要是兼顾易用性和隔离性。
 * 1. userId==0的case，不用每次每次都指定用户ID，并且有的地方就是用户ID上下文的，需要从全局uid取，
 *    这种情况，这里可以直接代劳（取全局uid)。
 * 2. 由于全局uid可能会切换，而切换uid的地方，大多数情况下是不感知系统的其他地方的状态的。
 *    如果有些地方是异步操作且时间较长，比如拉取用数据，如果拉取用户数据时所用的uid和拉取到数据时的全局uid不相等，
 *    则需要做措施应对。
 *    通常的做法是判断当前全局uid和请求时的uid不想等，则不保存。
 *    但是存在低概率的极限的情况，拉取到数据时判断还是相等的，然后将要取获取关联uid的存储实例时，用户ID切换了，
 *    那所取到的存储实例就时指向新uid的，那两个用户的数据就串了。
 *    因为切换id和保存数据不具备原子性，所以时有可能发生的。
 *    所以此处提供了userId入参，如果传入非零uid，则所取到的存储实例就是关联此uid的组件，从而避免写错用户空间。
 */
abstract class UserStorage(
    private val name: String,
    private val userId: Long
) : KVData() {
    companion object {
        private val CACHE = ArrayMap<String, LongSparseArray<FastKV>>()
    }

    private val group: LongSparseArray<FastKV>

    init {
        synchronized(CACHE) {
            group = CACHE.getOrPut(name) { LongSparseArray(2) }
        }
    }

    override val kv: FastKV
        get() {
            synchronized(group) {
                val uid = if (userId == 0L) AppContext.uid else userId
                var fastKV = group[uid]
                if (fastKV == null) {
                    val path = PathManager.fastKVDir + "/user/" + uidToDir(uid)
                    fastKV = buildKV(path, name)
                    group.put(uid, fastKV)
                }
                return fastKV
            }
        }

    protected fun closeFastKV(uid: Long) {
        synchronized(group) {
            val cache = group[uid]
            group.remove(uid)
            cache?.close()
        }
    }

    private fun uidToDir(uid: Long): String {
        val tag = AppContext.env.tag
        // 如有需要，可以对uid进行加密
//        val encryptUid = CipherManager.numberCipher.encryptLong(uid)
//        val uidStr = Utils.bytes2Hex(NumberCipher.long2Bytes(encryptUid))
//        return if (tag.isEmpty()) uidStr else "$uidStr-$tag"
        return if (tag.isEmpty()) "$uid" else "$uid-$tag"
    }
}