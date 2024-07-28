package io.fastkv.fastkvdemo.application

import android.app.Application
import android.content.Context
import android.util.Log
import com.tencent.mmkv.MMKV
import io.fastkv.FastKVConfig
import io.fastkv.fastkvdemo.BuildConfig
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.base.IAppContext
import io.fastkv.fastkvdemo.fastkv.utils.FastKVLogger
import io.fastkv.fastkvdemo.util.Utils
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor

class AppApplication : Application(), IAppContext {
    override val context: Context
        get() = this

    override val debug: Boolean
        get() = BuildConfig.DEBUG

    override val isMainProcess: Boolean
        get() = appId == null || appId == BuildConfig.APPLICATION_ID

    private var appId: String? = null

    override fun onCreate() {
        super.onCreate()
        // 第一件事，先初始APP上下文
        AppContext.init(this)

        appId = ProcessUtil.getProcessName(this)

        FastKVConfig.setLogger(FastKVLogger)
        FastKVConfig.setExecutor(Dispatchers.Default.asExecutor())

        // Avoid files corruption in multi-process environment
        if(isMainProcess && !Utils.is16KPageSize()) {
            initMMKV()
        }
    }

    private fun initMMKV(){
        /*
        val dir = filesDir.absolutePath + "/mmkv"
        val rootDir = MMKV.initialize(this, dir, {
                libName -> ReLinker.loadLibrary(this@MyApplication, libName)
            }, MMKVLogLevel.LevelInfo
        )*/
        val rootDir = MMKV.initialize(this)
        Log.i("MMKV", "mmkv root: $rootDir")
    }
}

