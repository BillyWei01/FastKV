package io.fastkv.fastkvdemo.base

import android.content.Context
import io.fastkv.fastkvdemo.data.AppState

/**
 * APP 全局上下文
 */
object AppContext : IAppContext {
    private lateinit var contextProxy: IAppContext

    var uid: Long = 0L

    // 如果有切换环境操作，最好重启一下APP(kill当前进程），
    // 以避免不同环境之间串数据。
    var env = Env.PPE

    fun init(appContext: IAppContext) {
        contextProxy = appContext
        uid = AppState.user_id
        env = AppState.environment
    }

    fun isLogin(): Boolean {
        return uid != 0L
    }

    override val context: Context
        get() = contextProxy.context

    override val debug: Boolean
        get() = contextProxy.debug

    override val isMainProcess: Boolean
        get() = contextProxy.isMainProcess
}