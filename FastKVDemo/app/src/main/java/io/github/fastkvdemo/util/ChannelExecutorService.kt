package io.github.fastkvdemo.util

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.TimeUnit

/**
 * ExecutorService with coroutines.
 *
 * The constructor's param 'capacity, refer to [Channel]'s constructor, control the concurrent size.
 *
 * - Channel.UNLIMITED: unlimited
 * - Channel.BUFFERED: decide by "channels.defaultBuffer",  which is 8, on current version.
 * - capacity = 1，serial
 * - capacity > 1, parallel
 *
 * However，[Dispatchers.IO] limit the concurrent size to 64,
 * for the case of Channel.UNLIMITED and capacity > 64，same effect to capacity=64.
 */
class ChannelExecutorService(capacity: Int) : AbstractExecutorService() {
    private val channel = Channel<Any>(capacity)

    /**
     * [command] will be execute by [Dispatchers.IO] finally.
     */
    override fun execute(command: Runnable) {
        channel.runBlock {
            command.run()
        }
    }

    fun isEmpty(): Boolean {
        return channel.isEmpty || channel.isClosedForReceive
    }

    /**
     * Note: calling [execute] after shutdown will throw [ClosedSendChannelException]
     */
    override fun shutdown() {
        channel.close()
    }

    /**
     * The method was not completely implement, DON'T USE THIS METHOD.
     */
    override fun shutdownNow(): MutableList<Runnable> {
        shutdown()
        return mutableListOf()
    }

    @ExperimentalCoroutinesApi
    override fun isShutdown(): Boolean {
        return channel.isClosedForSend
    }

    @ExperimentalCoroutinesApi
    override fun isTerminated(): Boolean {
        return channel.isClosedForReceive
    }

    override fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean {
        var millis = unit.toMillis(timeout)
        while (!isTerminated && millis > 0) {
            try {
                Thread.sleep(200L)
                millis -= 200L
            } catch (ignore: Exception) {
            }
        }
        return isTerminated
    }
}