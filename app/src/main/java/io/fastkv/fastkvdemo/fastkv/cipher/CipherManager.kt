package io.fastkv.fastkvdemo.fastkv.cipher

import android.os.Build
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.manager.PathManager
import io.fastkv.interfaces.FastCipher

object CipherManager {
    // It's suggest to load from somewhere else instead of hard-coding
    private val secretKey = "KeyStore!@#^%123".toByteArray()

    private val cipherSetting = FastKV.Builder(PathManager.fastKVDir, "cipher_setting")
        .blocking()
        .build()

    private const val USE_KEY_STORE = "use_key_store"

    fun getKVCipher(): FastCipher {
        // In case of the devices upgrade from version lower M to upper M,
        // saving the option of first time to keep app use the same cipher key.
        val overAndroidM = Build.VERSION.SDK_INT >= Build.VERSION_CODES.M
        if (!cipherSetting.contains(USE_KEY_STORE)) {
            cipherSetting.putBoolean(USE_KEY_STORE, overAndroidM)
        }
        return if (overAndroidM && cipherSetting.getBoolean(USE_KEY_STORE)) {
            AESCipher(KeyStoreHelper.getKey(secretKey))
        } else {
            AESCipher(secretKey)
        }
    }
}
