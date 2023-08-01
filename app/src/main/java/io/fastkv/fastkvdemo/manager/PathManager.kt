package io.fastkv.fastkvdemo.manager

import io.fastkv.fastkvdemo.base.AppContext

object PathManager {
    val filesDir: String = AppContext.context.filesDir.absolutePath
    val fastKVDir: String = "$filesDir/fastkv"
}