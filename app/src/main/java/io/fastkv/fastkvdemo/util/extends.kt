package io.fastkv.fastkvdemo.util

import android.os.SystemClock
import android.view.View
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

fun Channel<Any>.runBlock(block: suspend CoroutineScope.() -> Unit) {
    CoroutineScope(Dispatchers.Unconfined).launch {
        send(0)
        CoroutineScope(Dispatchers.IO).launch {
            block()
            receive()
        }
    }
}

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