package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.fastkv.LongListEncoder
import io.fastkv.fastkvdemo.fastkv.GroupStorage
import io.fastkv.fastkvdemo.fastkv.cipher.CipherManager
import io.fastkv.fastkvdemo.storage.CommonStorage
import io.fastkv.interfaces.FastCipher
import io.fastkv.interfaces.FastEncoder

object UserData : GroupStorage("user_data") {
    override fun groupId(): String {
       return CipherManager.numberCipher.encryptLong(CommonStorage.uid).toString()
    }

    override fun encoders(): Array<FastEncoder<*>> {
        return arrayOf(AccountInfo.ENCODER, LongListEncoder)
    }

    override fun cipher(): FastCipher {
        return CipherManager.defaultCipher
    }

    var userAccount by obj("user_account", AccountInfo.ENCODER)
    var gender by intEnum("gender", Gender.CONVERTER)
    var isVip by boolean("is_vip")
    var fansCount by int("fans_count")
    var score by float("score")
    var loginTime by long("login_time")
    var balance by double("balance")
    var sign by string("sing")
    var lock by array("lock")
    var tags by stringSet("tags")
    var favoriteChannels by obj("favorite_channels", LongListEncoder)
    val config by combineKey("config")
}
