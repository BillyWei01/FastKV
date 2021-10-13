package io.github.fastkvdemo.manager

import io.github.fastkvdemo.application.GlobalConfig

object PathManager {
    val filesDir: String = GlobalConfig.appContext.filesDir.absolutePath
    val fastKVDir: String = "$filesDir/fastkv"
}