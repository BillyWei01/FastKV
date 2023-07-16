package io.fastkv.fastkvdemo

import android.annotation.SuppressLint
import android.content.Intent
import android.graphics.Color
import android.os.Bundle
import android.view.View
import androidx.appcompat.app.AppCompatActivity
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.account.AccountManager
import io.fastkv.fastkvdemo.account.UserData
import io.fastkv.fastkvdemo.manager.PathManager
import io.fastkv.fastkvdemo.storage.SpCase
import io.fastkv.fastkvdemo.storage.KotlinCase
import io.fastkv.fastkvdemo.util.runBlock
import kotlinx.android.synthetic.main.activity_main.account_info_tv
import kotlinx.android.synthetic.main.activity_main.login_btn
import kotlinx.android.synthetic.main.activity_main.test_multi_process_btn
import kotlinx.android.synthetic.main.activity_main.test_performance_btn
import kotlinx.android.synthetic.main.activity_main.tips_tv
import kotlinx.android.synthetic.main.activity_main.user_info_tv
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

class MainActivity : AppCompatActivity() {
    companion object {
        private val serialChannel = Channel<Any>(1)
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        printLaunchTime()
        updateAccountInfo()

        login_btn.setOnClickListener {
            if (AccountManager.isLogin()) {
                AccountManager.logout()
            } else {
                AccountManager.login(10000)
            }
            updateAccountInfo()
        }

        test_multi_process_btn.setOnClickListener {
            val intent = Intent(this, MultiProcessTestActivity::class.java)
            startActivity(intent)
        }

        test_performance_btn.setOnClickListener {
            tips_tv.text = getString(R.string.running_tips)
            tips_tv.setTextColor(Color.parseColor("#FFFF8247"))
            test_performance_btn.isEnabled = false
            serialChannel.runBlock {
                Benchmark.start()
                GlobalScope.launch(Dispatchers.Main) {
                    tips_tv.text = getString(R.string.test_tips)
                    tips_tv.setTextColor(Color.parseColor("#FF009900"))
                    test_performance_btn.isEnabled = true
                }
            }
        }

        // Log.i("MyTag", "pageSize:" + Utils.getPageSize())
    }

    @SuppressLint("SetTextI18n")
    private fun updateAccountInfo() {
        if (AccountManager.isLogin()) {
            login_btn.text = getString(R.string.logout)
            account_info_tv.visibility = View.VISIBLE
            user_info_tv.visibility = View.VISIBLE
            UserData.userAccount?.run {
                account_info_tv.text =
                    "uid: $uid\nnickname: $nickname\nphone: $phoneNo\nemail: $email"
            }
            user_info_tv.text = AccountManager.formatUserInfo()
        } else {
            login_btn.text = getString(R.string.login)
            account_info_tv.visibility = View.GONE
            user_info_tv.visibility = View.GONE
        }
    }

    private fun printLaunchTime() {
        case1()
    }

    /**
     * If use SharedPreferences to store data before,
     * you could import old values to new store framework,
     * with the same interface with SharedPreferences.
     */
    fun case1() {
        val preferences = SpCase.preferences
        val t = preferences.getInt(SpCase.LAUNCH_COUNT, 0) + 1
        tips_tv.text = getString(R.string.main_tips, t)
        preferences.edit().putInt(SpCase.LAUNCH_COUNT, t).apply()
    }

    /**
     * If there is no data storing to SharedPreferences before.
     * just use new API of FastKV.
     */
    fun case2() {
        val kv = FastKV.Builder(PathManager.fastKVDir, "common_store").build()
        val t = kv.getInt("launch_count") + 1
        tips_tv.text = getString(R.string.main_tips, t)
        kv.putInt("launch_count", t)
    }

    /**
     * With kotlin's syntactic sugar, read/write key-value data just like accessing variable.
     */
    fun case3() {
        val t = KotlinCase.launchCount + 1
        tips_tv.text = getString(R.string.main_tips, t)
        KotlinCase.launchCount = t
    }
}