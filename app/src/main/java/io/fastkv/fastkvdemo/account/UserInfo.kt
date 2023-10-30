package io.fastkv.fastkvdemo.account

import android.util.ArrayMap
import io.fastkv.fastkvdemo.fastkv.LongListEncoder
import io.fastkv.fastkvdemo.fastkv.UserStorage
import io.fastkv.interfaces.FastEncoder

/**
 * 用户信息
 *
 * 封装一个既可以用默认用当前uid，又可以指定uid的API。
 */
object UserInfo : ShadowUserInfo(0L) {
    private val map = ArrayMap<Long, ShadowUserInfo>()

    @Synchronized
    fun get(uid: Long): ShadowUserInfo {
        return map.getOrPut(uid) {
            ShadowUserInfo(uid)
        }
    }

    @Synchronized
    fun close(uid: Long) {
        map.remove(uid)
        closeFastKV(uid)
    }
}

open class ShadowUserInfo(uid: Long) : UserStorage("user_info", uid) {
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
