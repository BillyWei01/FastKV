package io.fastkv.fastkvdemo.application

import android.app.Application
import android.util.Log
import com.tencent.mmkv.MMKV
import io.fastkv.FastKVConfig
import io.fastkv.fastkvdemo.fastkv.FastKVLogger
import io.fastkv.fastkvdemo.util.ChannelExecutor
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor

class AppApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        GlobalConfig.appContext = this

        FastKVConfig.setLogger(FastKVLogger)
        FastKVConfig.setExecutor(Dispatchers.Default.asExecutor())

        // filter other processes,
        // in case files damaged in multiprocess mode
        val processName = ProcessUtil.getProcessName(this)
        if(processName == null || processName == GlobalConfig.APPLICATION_ID) {
            initMMKV()
            // FastKV not support multiprocess (MPFastKV does)
            // preload()
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

/*    private fun preload() {
        GlobalScope.launch(Dispatchers.IO) {
            CommonStoreV2.kv
            UserData.kv
            FastKV.Builder(fastKVDir, "fastkv").build()
        }
    }*/
}

