package io.fastkv.fastkvdemo.util

/**
 * 双重校验锁-延时初始化包装类
 */
open class LazyInitWrapper<Param, T>(private val initializer: (param: Param) -> T) {
    @Volatile
    private var instance: T? = null

    fun get(param: Param): T {
        val currentInstance = instance
        if (currentInstance != null) {
            return currentInstance
        }
        synchronized(this) {
            if (instance == null) {
                instance = initializer.invoke(param)
            }
            return instance!!
        }
    }
}
