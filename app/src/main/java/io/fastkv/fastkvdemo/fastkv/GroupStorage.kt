package io.fastkv.fastkvdemo.fastkv

import android.util.ArrayMap
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.manager.PathManager

/**
 * 有时候我们需要根据分组分隔目录去存储数据，例如：
 * 1. 根据用户ID
 * 2. 根据服务器环境（正式服/测试服，或者线上环境/开发环境）
 * 3. 或者以上的组合
 *
 * 重载 'groupId' 并提供group id, 这样当前kv的数据就会分布在不同的实例了。
 */
open class GroupStorage(private val name: String) : KVData() {
    companion object {
        private val kvMap = ArrayMap<String, HashMap<String, FastKV>>()
    }

    protected open val groupId: String = AppContext.env.tag

    override val kv: FastKV
        get() {
            val gid = groupId
            synchronized(kvMap) {
                val group = kvMap.getOrPut(gid) {
                    HashMap()
                }
                return group.getOrPut(name) {
                    val path = PathManager.fastKVDir + "/group/" + gid
                    buildKV(path, name)
                }
            }
        }

    protected fun closeFastKV(gid: String) {
        synchronized(kvMap) {
            kvMap[gid]?.let { group ->
                // 执行FastKV的close, FastKV会将实例移除出MAP。
                // 这里也要从group同步移除；
                // 否则下次索引到的就是一个已经关闭的kv，而已经关闭的kv是不能更新数据的。
                group.remove(name)?.close()
            }
        }
    }
}
