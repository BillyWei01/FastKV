package io.fastkv.fastkvdemo.fastkv.cipher

import android.os.Build
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.manager.PathManager

object CipherManager {
    val defaultCipher: AESCipher = getKVCipher()
    val numberCipher: NumberCipher = defaultCipher.numberCipher

    private fun getKVCipher(): AESCipher {
       val cipherSetting = FastKV.Builder(PathManager.fastKVDir, "cipher_setting")
            .blocking()
            .build()

        // In case of the devices upgrade from version lower M to upper M,
        // saving the option of first time to make app keep using the same cipher key.
        val overAndroidM = Build.VERSION.SDK_INT >= Build.VERSION_CODES.M
        val key = "used_key_store"
        val usedKeyStore = if (cipherSetting.contains(key)) {
            cipherSetting.getBoolean(key)
        } else {
            cipherSetting.putBoolean(key, overAndroidM)
            overAndroidM
        }
        cipherSetting.close()

        val seed = "1234567890abcdef1234567890ABCDEF".toByteArray()
        return if (usedKeyStore && overAndroidM) {
            AESCipher(KeyStoreHelper.getKey(seed))
        } else {
            AESCipher(seed)
        }
    }
}
