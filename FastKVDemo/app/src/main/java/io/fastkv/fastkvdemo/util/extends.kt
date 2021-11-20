package io.fastkv.fastkvdemo.util

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