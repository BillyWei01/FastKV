package io.fastkv.fastkvdemo.fastkv

import android.util.ArrayMap
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.manager.PathManager
import io.fastkv.fastkvdemo.util.Digest

abstract class UserStorage(private val name: String) : KVData() {
    companion object {
        val kvMap = ArrayMap<String, HashMap<String, FastKV>>()
    }

    protected abstract fun getUid(): String

    override val kv: FastKV
        get() {
            synchronized(kvMap) {
                val uid = getUid()
                val group = kvMap.getOrPut(uid) {
                    HashMap()
                }
                val kv = group.getOrPut(name) {
                    // Make data save in different folder (group by uid).
                    val path = PathManager.fastKVDir + "/user/" + uid
                    buildKV(path, name)
                }
                return kv
            }
        }
}
