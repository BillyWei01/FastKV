package io.fastkv.fastkvdemo.manager

import io.fastkv.fastkvdemo.application.GlobalConfig

object PathManager {
    val filesDir: String = GlobalConfig.appContext.filesDir.absolutePath
    val fastKVDir: String = "$filesDir/fastkv"
}