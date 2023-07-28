package io.fastkv.fastkvdemo.storage

import io.fastkv.fastkvdemo.fastkv.GlobalStorage
import io.fastkv.fastkvdemo.fastkv.cipher.CipherManager
import io.fastkv.interfaces.FastCipher

object CommonStorage : GlobalStorage("common_storage") {
    override fun cipher(): FastCipher {
        return CipherManager.getKVCipher()
    }

    var launchCount by int("launch_count")
    var deviceId by string("device_id")
    var installId by string("install_id")
    var uid by string("user_id")
    val setting by map("setting")
}