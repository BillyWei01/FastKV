package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.fastkv.LongListEncoder
import io.fastkv.fastkvdemo.fastkv.UserStorage
import io.fastkv.fastkvdemo.fastkv.cipher.CipherManager
import io.fastkv.fastkvdemo.storage.CommonStorage
import io.fastkv.interfaces.FastCipher
import io.fastkv.interfaces.FastEncoder

object UserData : UserStorage("user_data") {
    override fun getUid(): String {
       return CommonStorage.uid
    }

    override fun encoders(): Array<FastEncoder<*>> {
        return arrayOf(AccountInfo.ENCODER, LongListEncoder)
    }

    override fun cipher(): FastCipher {
        return CipherManager.getKVCipher()
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
    val config by map("config")
}
