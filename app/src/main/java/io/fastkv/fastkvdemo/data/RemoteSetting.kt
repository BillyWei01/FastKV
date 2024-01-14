package io.fastkv.fastkvdemo.data

import io.fastkv.fastkvdemo.fastkv.kvbase.UserKV

/**
 * 远程设置
 *
 * 从服务端拉取的配置项，需要区分环境，但是和用户无关。
 * 所以，uid固定为0（其他的uid用不到的数值也可以）。
 */
object RemoteSetting : UserKV("remote_setting", 0L) {
    val showGuide by boolean("show_guide")

    val setting by combineKey("setting")
}