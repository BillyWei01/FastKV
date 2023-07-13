package io.fastkv.fastkvdemo.account

import android.os.Build
import io.fastkv.interfaces.FastCipher
import io.fastkv.fastkvdemo.fastkv.AESCipher
import io.fastkv.fastkvdemo.fastkv.KVData
import io.fastkv.fastkvdemo.fastkv.KeyStoreHelper
import io.fastkv.fastkvdemo.fastkv.LongListEncoder
import io.fastkv.fastkvdemo.storage.CommonStoreV2
import io.fastkv.interfaces.FastEncoder

object UserData : KVData("user_data") {
    // It's suggest to load from somewhere else instead of hard-coding
    private val secretKey = "KeyStore!@#^%123".toByteArray()

    //  private static final String INIT_KEY = "KeyStore!@#$%123";

    override fun encoders(): Array<FastEncoder<*>> {
        return arrayOf(AccountInfo.ENCODER, LongListEncoder)
    }

    override fun cipher(): FastCipher {
        // In case of the devices upgrade from version lower M to upper M,
        // saving the option of first time to keep app use the same cipher key.
        val overAndroidM = Build.VERSION.SDK_INT >= Build.VERSION_CODES.M
        if (!CommonStoreV2.contains(CommonStoreV2.USE_KEY_STORE)) {
            CommonStoreV2.useKeyStore = overAndroidM
        }
        return if (overAndroidM && CommonStoreV2.useKeyStore) {
            // Log.i("FastKV", "cipher key: " + Utils.bytes2Hex(KeyStoreHelper.getKey()))
            AESCipher(KeyStoreHelper.getKey(secretKey))
        } else {
            AESCipher(secretKey)
        }
    }

    var userAccount by obj("user_account", AccountInfo.ENCODER)
    var isVip by boolean("is_vip")
    var fansCount by int("fans_count")
    var score by float("score")
    var loginTime by long("login_time")
    var balance by double("balance")
    var sign by string("sing")
    var lock by array("lock")
    var tags by stringSet("tags")
    var favoriteChannels by obj("favorite_channels", LongListEncoder)
}
