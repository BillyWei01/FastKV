package io.fastkv.fastkvdemo

import android.content.ComponentName
import android.os.Bundle
import android.util.Log
import androidx.appcompat.app.AppCompatActivity
import io.fastkv.MPFastKV
import io.fastkv.fastkvdemo.manager.PathManager.fastKVDir
import kotlinx.android.synthetic.main.activity_multi_process_test.message_tv
import android.content.Intent
import android.content.ServiceConnection
import android.content.SharedPreferences
import android.os.IBinder
import android.os.Message
import android.os.Messenger
import io.fastkv.fastkvdemo.event.EventManager
import io.fastkv.fastkvdemo.event.Events
import io.fastkv.fastkvdemo.event.Observer
import kotlinx.android.synthetic.main.activity_multi_process_test.client_input_btn
import kotlinx.android.synthetic.main.activity_multi_process_test.server_input_btn


class MultiProcessTestActivity : AppCompatActivity(), Observer {
    companion object {
        private const val TAG = "TestActivity"

        private val mpTestKv = MPFastKV.Builder(fastKVDir, "mp_test_kv").build()

        private var serverCount = mpTestKv.getInt("server")
        private var clientCount = mpTestKv.getInt("client")

        private val listener = SharedPreferences.OnSharedPreferenceChangeListener { sp, key ->
            if (key == null) {
                Log.i(TAG, "clear")
            } else {
                if (key == "server" || key == "client") {
                    val value = sp.getInt(key, 0)
                    Log.i(TAG, "data change, key:$key value:$value")
                }
            }
            EventManager.notify(Events.MP_TEST_KV_CHANGE)
        }

        init {
            mpTestKv.registerOnSharedPreferenceChangeListener(listener)
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

        message_tv.text = mpTestKv.all.toString()

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
        mpTestKv.putInt("client", clientCount).commit()
    }

    override fun onDestroy() {
        super.onDestroy()
        unbindService(connection)
        EventManager.unregister(this)
    }

    override fun onEvent(event: Int, vararg args: Any?) {
        if (event == Events.MP_TEST_KV_CHANGE) {
            message_tv.text = mpTestKv.all.toString()
        }
    }

    override fun listenEvents(): IntArray {
        return intArrayOf(Events.MP_TEST_KV_CHANGE)
    }
}
