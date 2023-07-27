package io.fastkv.fastkvdemo.fastkv

import android.util.ArrayMap
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.manager.PathManager
import io.fastkv.fastkvdemo.util.Digest

abstract class UserStorage(val name: String) : KVData() {
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
                    buildFastKV(uid)
                }
                return kv
            }
        }

    private fun buildFastKV(uid: String): FastKV {
        // Hide uid to digest
        val uidDigest = Digest.md5(uid.toByteArray())

        // Make data of users saving in different folder.
        val path = PathManager.fastKVDir + "/" + uidDigest

        return FastKV.Builder(path, name)
            .encoder(encoders())
            .cipher(cipher())
            .build()
    }
}
