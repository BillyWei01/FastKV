package io.fastkv.fastkvdemo

import android.content.ComponentName
import android.content.Context.BIND_AUTO_CREATE
import android.content.Intent
import android.content.ServiceConnection
import android.content.SharedPreferences
import android.os.Bundle
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.Message
import android.os.Messenger
import android.util.Log
import androidx.appcompat.app.AppCompatActivity
import io.fastkv.MPFastKV
import io.fastkv.fastkvdemo.event.EventManager
import io.fastkv.fastkvdemo.event.Events
import io.fastkv.fastkvdemo.event.Observer
import io.fastkv.fastkvdemo.manager.PathManager.fastKVDir
import java.lang.Integer.min
import kotlinx.android.synthetic.main.activity_multi_process_test.client_input_btn
import kotlinx.android.synthetic.main.activity_multi_process_test.message_tv
import kotlinx.android.synthetic.main.activity_multi_process_test.server_input_btn


class MultiProcessTestActivity : AppCompatActivity(), Observer {
    companion object {
        private const val TAG = "TestActivity"

        private val mpTestKv = MPFastKV.Builder(fastKVDir, "mp_test_kv")
            .build()

        private var serverCount = mpTestKv.getInt("server")
        private var clientCount = mpTestKv.getInt("client")

        private const val MSG_LOG_ALL = 101

        private val mainHandler = object : Handler(Looper.getMainLooper()) {
            override fun handleMessage(msg: Message) {
                if (msg.what == MSG_LOG_ALL) {
                    Log.d(TAG, "all values:" + getAll())
                }
            }
        }

        private val listener = SharedPreferences.OnSharedPreferenceChangeListener { sp, key ->
            if (key == null) {
                Log.i(TAG, "clear")
            } else {
                Log.i(TAG, "data change, key:$key")
                if (!mainHandler.hasMessages(MSG_LOG_ALL)) {
                    mainHandler.sendEmptyMessageDelayed(MSG_LOG_ALL, 100L)
                }
            }
            EventManager.notify(Events.MP_TEST_KV_CHANGE)
        }

        init {
            mpTestKv.registerOnSharedPreferenceChangeListener(listener)
        }

        private fun getAll(): String {
            return mpTestKv.all.map {
                it.key + "=" + (it.value.let { value ->
                    if (value is String) {
                        if (value.length > 10) {
                            value.substring(0, min(10, value.length)) + "..."
                        } else {
                            value
                        }
                    } else {
                        value
                    }
                })
            }.toString()
        }
    }

    private var testService: IBinder? = null

    private val connection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
            testService = service
        }

        override fun onServiceDisconnected(name: ComponentName?) {
        }
    };

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_multi_process_test)

        message_tv.text = getAll()

        val intent = Intent(this, TestService::class.java)
        bindService(intent, connection, BIND_AUTO_CREATE)

        server_input_btn.setOnClickListener {
            serverInput()
        }

        client_input_btn.setOnClickListener {
            clientInput()
        }

        EventManager.register(this)
    }

    private fun serverInput() {
        testService?.let {
            serverCount++
            val messenger = Messenger(it)
            val message = Message.obtain()
            val bundle = Bundle()
            bundle.putString("key", "server")
            bundle.putInt("value", serverCount)
            message.data = bundle
            messenger.send(message)
        }
    }

    private fun clientInput() {
        clientCount++
        val ch = if (clientCount % 2 == 0) 'a' else 'b'
        val diff = clientCount % 10
        val bytes = ByteArray(2030 + diff)
        bytes.fill(ch.code.toByte(), 0, bytes.size)
        val str = String(bytes)
        mpTestKv
            .putInt("client", clientCount)
            .putString("str_1", str)
            .putString("str_2", str)
            .commit()
    }

    override fun onDestroy() {
        super.onDestroy()
        unbindService(connection)
        EventManager.unregister(this)
    }

    override fun onEvent(event: Int, vararg args: Any?) {
        if (event == Events.MP_TEST_KV_CHANGE) {
            message_tv.text = getAll()
        }
    }


    override fun listenEvents(): IntArray {
        return intArrayOf(Events.MP_TEST_KV_CHANGE)
    }
}
