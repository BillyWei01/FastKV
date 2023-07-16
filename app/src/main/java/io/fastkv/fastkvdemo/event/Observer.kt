package io.fastkv.fastkvdemo.event

interface Observer {
    fun onEvent(event: Int, vararg args : Any?)
    fun listenEvents(): IntArray
}