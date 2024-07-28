package io.fastkv.fastkvdemo

import android.annotation.SuppressLint
import android.content.Intent
import android.graphics.Color
import android.os.Bundle
import android.util.Log
import android.view.View
import android.widget.Button
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.account.AccountManager
import io.fastkv.fastkvdemo.account.UserInfo
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.manager.PathManager
import io.fastkv.fastkvdemo.data.SpCase
import io.fastkv.fastkvdemo.data.UsageData
import io.fastkv.fastkvdemo.data.UserSetting
import io.fastkv.fastkvdemo.util.onClick
import io.fastkv.fastkvdemo.util.runBlock
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

class MainActivity : AppCompatActivity() {
    companion object {
        private val serialChannel = Channel<Any>(1)
        const val TAG = "MainActivity"
    }

    var lastUid = 0L


    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        printLaunchTime()
        refreshAccountInfoViews()

        val switch_account_btn = findViewById<Button>(R.id.switch_account_btn)
        val tips_tv = findViewById<TextView>(R.id.tips_tv)

        findViewById<Button>(R.id.login_btn).onClick {
            if (AccountManager.isLogin()) {
                lastUid = AppContext.uid
                AccountManager.logout()
                switch_account_btn.isEnabled = false

                UserSetting.flags["logged"] = false
            } else {
                if (lastUid == 0L) {
                    AccountManager.login(10001L)
                } else {
                    AccountManager.login(lastUid)
                }
                switch_account_btn.isEnabled = true


                UserSetting.flags["logged"] = true
            }
            refreshAccountInfoViews()
        }

        findViewById<Button>(R.id.switch_account_btn).onClick {
            if (AppContext.isLogin()) {
                if (AppContext.uid == 10001L) {
                    AccountManager.switchAccount(10002L)
                } else {
                    AccountManager.switchAccount(10001L)
                }
            } else {
                tips_tv.text = getString(R.string.login_first_tips)
            }
            refreshAccountInfoViews()
        }

        // Test map
        val logged = UserSetting.flags["logged"]
        Log.d(TAG, "logged:$logged")

        // Test 'remove'
        // UserData.config.remove("notification")

        findViewById<Button>(R.id.test_multi_process_btn).onClick {
            val intent = Intent(this, MultiProcessTestActivity::class.java)
            startActivity(intent)
        }

        findViewById<Button>(R.id.test_performance_btn).onClick {
            startBenchMark()
        }
    }

    @SuppressLint("SetTextI18n")
    private fun startBenchMark() {
        val tips_tv = findViewById<TextView>(R.id.tips_tv)
        val test_performance_btn = findViewById<Button>(R.id.test_performance_btn)

        tips_tv.text = getString(R.string.running_tips)
        tips_tv.setTextColor(Color.parseColor("#FFFF8247"))
        test_performance_btn.isEnabled = false
        serialChannel.runBlock {
            Benchmark.start { kvCount ->
                CoroutineScope(Dispatchers.Main).launch {
                    if (kvCount >= 0) {
                        tips_tv.text = "Testing, kvCount: $kvCount"
                    } else {
                        tips_tv.text = getString(R.string.test_tips)
                        test_performance_btn.isEnabled = true
                    }
                    tips_tv.setTextColor(Color.parseColor("#FF009900"))
                }
            }
        }
    }

    @SuppressLint("SetTextI18n")
    private fun refreshAccountInfoViews() {
        val account_info_tv = findViewById<TextView>(R.id.account_info_tv)
        val login_btn = findViewById<TextView>(R.id.login_btn)
        val user_info_tv = findViewById<TextView>(R.id.user_info_tv)

        if (AccountManager.isLogin()) {
            login_btn.text = getString(R.string.logout)
            account_info_tv.visibility = View.VISIBLE
            user_info_tv.visibility = View.VISIBLE
            UserInfo.userAccount?.run {
                //val uid = CommonStorage.uid
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
        findViewById<TextView>(R.id.tips_tv).text = getString(R.string.main_tips, t)
        preferences.edit().putInt(SpCase.LAUNCH_COUNT, t).apply()
    }

    /**
     * If there is no data storing to SharedPreferences before.
     * just use new API of FastKV.
     */
    fun case2() {
        val kv = FastKV.Builder(PathManager.fastKVDir, "common_store").build()
        val t = kv.getInt("launch_count") + 1
        findViewById<TextView>(R.id.tips_tv).text = getString(R.string.main_tips, t)
        kv.putInt("launch_count", t)
    }

    /**
     * With kotlin's syntactic sugar, read/write key-value data just like accessing variable.
     */
    fun case3() {
        val t = UsageData.launchCount + 1
        findViewById<TextView>(R.id.tips_tv).text = getString(R.string.main_tips, t)
        UsageData.launchCount = t
    }
}