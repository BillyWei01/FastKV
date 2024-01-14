package io.fastkv.fastkvdemo.data

import io.fastkv.fastkvdemo.base.Env
import io.fastkv.fastkvdemo.fastkv.kvbase.GlobalKV

/**
 * APP信息
 */
object AppState : GlobalKV("app_state") {
    // 服务器环境
    var environment by stringEnum("environment", Env.CONVERTER)

    // 用户ID
    var userId by long("user_id")

    // 设备ID
    var deviceId by string("device_id")
}