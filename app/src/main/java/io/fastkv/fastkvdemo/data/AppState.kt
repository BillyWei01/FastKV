package io.fastkv.fastkvdemo.data

import io.fastkv.fastkvdemo.base.Env
import io.fastkv.fastkvdemo.fastkv.GlobalStorage

/**
 * 本地信息，不需要同步。
 */
object AppState : GlobalStorage("app_state") {
    // 服务器环境
    var environment by stringEnum("environment", Env.CONVERTER)

    // 用户ID
    var user_id by long("user_id");

    // 设备ID
    var deviceId by string("device_id")
}