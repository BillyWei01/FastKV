package io.fastkv.fastkvdemo.fastkv

import android.util.ArrayMap
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.manager.PathManager

/**
 * Sometime we need to separate directory by group, like:
 * 1. user id.
 * 2. official server and test server.
 *
 * Override 'groupId()' and provider the group id, <br>
 * the kv (both memory cache and file) will be separated.
 */
abstract class GroupStorage(private val name: String) : KVData() {
    companion object {
        val kvMap = ArrayMap<String, HashMap<String, FastKV>>()
    }

    protected abstract fun groupId(): String

    override val kv: FastKV
        get() {
            synchronized(kvMap) {
                val gid = groupId()
                val group = kvMap.getOrPut(gid) {
                    HashMap()
                }
                val kv = group.getOrPut(name) {
                    val path = PathManager.fastKVDir + "/group/" + gid
                    buildKV(path, name)
                }
                return kv
            }
        }
}
