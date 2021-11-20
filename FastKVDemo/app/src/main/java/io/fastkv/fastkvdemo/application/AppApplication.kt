package io.fastkv.fastkvdemo.application

import android.app.Application
import android.util.Log
import com.tencent.mmkv.MMKV
import io.fastkv.FastKV
import io.fastkv.FastKVConfig
import io.fastkv.fastkvdemo.account.UserData
import io.fastkv.fastkvdemo.fastkv.FastKVLogger
import io.fastkv.fastkvdemo.manager.PathManager.fastKVDir
import io.fastkv.fastkvdemo.storage.CommonStoreV2
import io.fastkv.fastkvdemo.util.ChannelExecutorService
import io.fastkv.fastkvdemo.util.runBlock
import kotlinx.coroutines.channels.Channel

class AppApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        GlobalConfig.appContext = this

        // filter other processes,
        // in case files damaged in multiple processes mode
        val processName = ProcessUtil.getProcessName(this)
        if(processName == null || processName == GlobalConfig.APPLICATION_ID) {
            init()
            initMMKV()
        }
    }

    private fun init() {
        FastKVConfig.setLogger(FastKVLogger)
        FastKVConfig.setExecutor(ChannelExecutorService(4))
        preload()
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

    private fun preload() {
        Channel<Any>(1).runBlock {
            CommonStoreV2.kv
            UserData.kv
            FastKV.Builder(fastKVDir, "fastkv").build()
        }
    }
}

