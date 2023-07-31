package io.fastkv.fastkvdemo.storage

import io.fastkv.fastkvdemo.fastkv.GlobalStorage
import io.fastkv.fastkvdemo.fastkv.cipher.CipherManager
import io.fastkv.interfaces.FastCipher

object CommonStorage : GlobalStorage("common_storage") {
    override fun cipher(): FastCipher {
        return CipherManager.defaultCipher
    }

    var launchCount by int("launch_count")
    var deviceId by string("device_id")
    var installId by string("install_id")
    var uid by long("user_id")
    val setting by combineKey("setting")
}