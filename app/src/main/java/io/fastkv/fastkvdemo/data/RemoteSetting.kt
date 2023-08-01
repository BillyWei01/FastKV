package io.fastkv.fastkvdemo.data

import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.fastkv.GroupStorage

/**
 * 远程设置
 *
 * 从服务端拉取的配置项，需要区分环境, 和用户无关
 */
object RemoteSetting : GroupStorage("remote_setting") {
    override val groupId
        get() = AppContext.env.tag

    val showGuide by boolean("show_guide")

    val setting by combineKey("setting")
}