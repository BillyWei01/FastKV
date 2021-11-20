package io.fastkv.fastkvdemo.account

import io.fastkv.FastKV
import io.fastkv.fastkvdemo.fastkv.KVData
import io.fastkv.fastkvdemo.fastkv.LongListEncoder

object UserData: KVData("user_data") {
    override fun encoders(): Array<FastKV.Encoder<*>> {
        return arrayOf(AccountInfo.ENCODER, LongListEncoder)
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