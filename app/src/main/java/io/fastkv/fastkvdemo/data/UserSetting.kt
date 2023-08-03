package io.fastkv.fastkvdemo.data

import android.util.ArrayMap
import io.fastkv.fastkvdemo.fastkv.UserStorage

/**
 * 用户设置（使用偏好）
 */
object UserSetting : ShadowUserSetting(0) {
    private val map = ArrayMap<Long, ShadowUserSetting>()

    @Synchronized
    fun get(uid: Long): ShadowUserSetting {
        return map.getOrPut(uid) {
            ShadowUserSetting(uid)
        }
    }

    @Synchronized
    fun close(uid: Long) {
        map.remove(uid)
        closeFastKV(uid)
    }
}

open class ShadowUserSetting(uid: Long) : UserStorage("user_setting", uid) {
    val lock by array("lock")
}
