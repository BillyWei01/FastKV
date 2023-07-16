package io.fastkv.fastkvdemo.storage

import io.fastkv.fastkvdemo.fastkv.KVData

object KotlinCase : KVData("common_store") {
    var launchCount by int("launch_count")
    var deviceId by string("device_id")
    var installId by string("install_id")
}