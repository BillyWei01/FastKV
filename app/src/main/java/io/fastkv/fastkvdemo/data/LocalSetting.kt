package io.fastkv.fastkvdemo.data

import io.fastkv.fastkvdemo.fastkv.GlobalStorage


/**
 * App本地设置（配置项）
 * 离线数据，不需要同步，不需要区分环境
 */
object LocalSetting : GlobalStorage("local_setting") {
    // 是否开启开发者入口
    var enableDeveloper by boolean("enable_developer")
}