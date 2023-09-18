package io.fastkv.fastkvdemo.util

import android.os.SystemClock
import android.view.View

fun View.onClick(interval: Long = 300L, block: () -> Unit) {
    var lastTime = 0L
    this.setOnClickListener {
        val now = SystemClock.elapsedRealtime()
        if ((now - lastTime) >= interval) {
            lastTime = now
            block()
        }
    }
}