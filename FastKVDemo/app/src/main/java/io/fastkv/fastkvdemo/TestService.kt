package io.fastkv.fastkvdemo

import android.app.Service
import android.content.Intent
import android.content.SharedPreferences
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.Message
import android.os.Messenger
import android.util.Log
import io.fastkv.MPFastKV
import io.fastkv.fastkvdemo.manager.PathManager

class TestService : Service() {
    private val mpTestKv = MPFastKV.Builder(PathManager.fastKVDir, "mp_test_kv").build()

    private val handler: Handler = object : Handler(Looper.getMainLooper()) {
        override fun handleMessage(msg: Message) {
            super.handleMessage(msg)
            val bundle = msg.data
            val key = bundle.getString("key")
            val value = bundle.getInt("value")
            Log.d(TAG, "message, key:$key value:$value")
            mpTestKv.putInt(key, value).commit()
        }
    }

    private val listener = SharedPreferences.OnSharedPreferenceChangeListener { sp, key ->
            if (key == null) {
                Log.i(TAG, "clear")
            } else {
                if (key == "server" || key == "client") {
                    val value = sp.getInt(key, 0)
                    Log.i(TAG, "data change, key:$key value:$value")
                }
            }
        }

    private val messenger = Messenger(handler)

    override fun onBind(intent: Intent): IBinder? {
        Log.d(TAG, "TestService onBind")
        mpTestKv.registerOnSharedPreferenceChangeListener (listener)
        return messenger.binder
    }

    override fun onUnbind(intent: Intent?): Boolean {
        Log.d(TAG, "TestService onUnbind")
        mpTestKv.unregisterOnSharedPreferenceChangeListener(listener)
        return super.onUnbind(intent)
    }

    companion object {
        private const val TAG = "TestService"
    }
}