package io.fastkv.fastkvdemo.fastkv

import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.fastkv.cipher.CipherManager
import io.fastkv.fastkvdemo.fastkv.cipher.NumberCipher
import io.fastkv.fastkvdemo.util.Utils

/**
 * 用户数据存储，同时区分环境和用户ID
 *
 * 切换用户时，会自动切换groupId。
 * 切换账号时，需注意停止用户相关的后台任务，尤其是设计有“写入数据”的任务。
 * 否则可能会发生“切换用户前的后台任务获取的数据，写到当前用户的存储空间”的问题。
 */
open class UserStorage(name: String) : GroupStorage(name) {
    @Volatile
    private var currentUid = -1L

    private var gid = AppContext.env.tag

    override val groupId: String
        get() {
            val uid = AppContext.uid
            if (currentUid != uid) {
                updateGroup(uid)
            }
            return gid
        }

    @Synchronized
    private fun updateGroup(uid: Long) {
        // Double check
        if (currentUid != uid) {
            val tag = AppContext.env.tag
            if (currentUid >= 0) {
                // 切换账号时，如果之前有登陆账号，则关闭之前的KV。
                // 此操作为可选项：
                // 1. 如果关闭，可以释放之前的KV的内存
                // 2. 如果不关闭，切换回来时能直接读取
                closeFastKV(uidToGid(currentUid, tag))
            }
            gid = uidToGid(uid, tag)
            currentUid = uid
        }
    }

    private fun uidToGid(uid: Long, tag: String): String {
        // 先加密uid, 再用来构建groupId(会作为文件路径的一部分）
        val encryptUid = CipherManager.numberCipher.encryptLong(uid)
        val uidStr = Utils.bytes2Hex(NumberCipher.long2Bytes(encryptUid))
        return if (tag.isEmpty()) uidStr else "$tag-$uidStr"
    }
}