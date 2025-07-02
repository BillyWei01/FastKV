package io.fastkv.fastkvdemo.fastkv.cipher

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.manager.PathManager

object CipherManager {
    val defaultCipher: AESCipher = getKVCipher()
    // val numberCipher: NumberCipher = defaultCipher.numberCipher

    private fun getKVCipher(): AESCipher {
       val cipherSetting = FastKV.Builder(PathManager.fastKVDir, "cipher_setting")
            .blocking()
            .build()
        var encryptKey: ByteArray
        // KeyStore 有时候不太稳定，所以通过持久化来确保加密key的稳定性（每次都能获取到相同的key)
        val key = cipherSetting.getArray("key")
        if (key == null) {
            val seed = "1234567890abcdef1234567890ABCDEF".toByteArray()
            encryptKey = KeyStoreHelper.getKey(seed)
            cipherSetting.putArray("key", encryptKey)
        } else {
            encryptKey = key
        }
        cipherSetting.close()
        return AESCipher(encryptKey)
    }
}
