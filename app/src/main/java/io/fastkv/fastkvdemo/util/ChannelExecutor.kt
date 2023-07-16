package io.fastkv.fastkvdemo.util

import kotlinx.coroutines.channels.Channel
import java.util.concurrent.Executor

class ChannelExecutor(capacity: Int) : Executor {
    private val channel = Channel<Any>(capacity)

    override fun execute(command: Runnable) {
        channel.runBlock {
            command.run()
        }
    }
}