package io.fastkv.fastkvdemo

import android.app.Service
import android.content.Intent
import android.content.SharedPreferences
import android.os.Build.VERSION_CODES.M
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.Message
import android.os.Messenger
import android.util.Log
import io.fastkv.MPFastKV
import io.fastkv.fastkvdemo.manager.PathManager

class TestService : Service() {

    companion object {
        private const val TAG = "TestService"
        private const val MSG_LOG_ALL = 102
    }

    private val mpTestKv = MPFastKV.Builder(PathManager.fastKVDir, "mp_test_kv").build()

    private val handler: Handler = object : Handler(Looper.getMainLooper()) {
        override fun handleMessage(msg: Message) {
            if (msg.what == MSG_LOG_ALL) {
                Log.d(TAG, "all values:" + getAll())
            } else {
                val bundle = msg.data
                val key = bundle.getString("key")
                val value = bundle.getInt("value")
                Log.d(TAG, "message, key:$key value:$value")
                mpTestKv.putInt(key, value).apply()
            }
        }
    }

    private val listener = SharedPreferences.OnSharedPreferenceChangeListener { sp, key ->
        if (key == null) {
            Log.i(TAG, "clear")
        } else {
            Log.i(TAG, "data change, key:$key")
            if (!handler.hasMessages(MSG_LOG_ALL)) {
                handler.sendEmptyMessageDelayed(MSG_LOG_ALL, 100L)
            }
        }
    }

    private val messenger = Messenger(handler)

    override fun onBind(intent: Intent): IBinder? {
        Log.d(TAG, "TestService onBind")
        mpTestKv.registerOnSharedPreferenceChangeListener(listener)
        return messenger.binder
    }

    private fun getAll(): String {
        return mpTestKv.all.map {
            it.key + "=" + (it.value.let { value ->
                if (value is String) {
                    if (value.length > 10) {
                        value.substring(0, Integer.min(10, value.length)) + "..."
                    } else {
                        value
                    }
                } else {
                    value
                }
            })
        }.toString()
    }

    override fun onUnbind(intent: Intent?): Boolean {
        Log.d(TAG, "TestService onUnbind")
        mpTestKv.unregisterOnSharedPreferenceChangeListener(listener)
        return super.onUnbind(intent)
    }
}