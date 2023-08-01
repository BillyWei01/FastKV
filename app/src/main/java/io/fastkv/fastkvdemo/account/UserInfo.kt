package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.fastkv.LongListEncoder
import io.fastkv.fastkvdemo.fastkv.UserStorage
import io.fastkv.interfaces.FastEncoder

/**
 * 用户信息
 */
object UserInfo : UserStorage("user_data") {
    override fun encoders(): Array<FastEncoder<*>> {
        return arrayOf(AccountInfo.ENCODER, LongListEncoder)
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
