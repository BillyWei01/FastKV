package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.util.Utils
import io.fastkv.fastkvdemo.data.AppState
import java.lang.StringBuilder
import kotlin.random.Random

object AccountManager {
    fun isLogin(): Boolean {
        return AppContext.uid != 0L
    }

    /**
     * 切换到注销状态前，最好发送一下"注销“事件给存在“写入用户数据”的异步任务，
     * 如果这些任务正在执行，则取消之。
     * 具体原因可参考：[io.fastkv.fastkvdemo.fastkv.UserStorage]
     */
    fun logout(): Boolean {
        // Post“注销”的消息（这里就不演示了）

        AppContext.uid = 0L
        AppState.user_id = 0L
        return true
    }

    fun login(uid: Long) {
        if (!AppContext.isLogin() || logout()) {
            doLogin(uid)
        }
    }

    fun switchAccount(uid: Long) {
        if (AppContext.uid != uid && logout()) {
            doLogin(uid)
        }
    }

    private fun doLogin(uid: Long) {
        // Do some thing about login

        // mock data
        val accountInfo = AccountInfo(
            uid,
            "mock token",
            "hello",
            "12312345678",
            "u$uid@gmail.com"
        )
        onSuccess(accountInfo)
    }

    private fun onSuccess(accountInfo: AccountInfo) {
        // 首先第一件事情就是切换上下AppContext中的uid,
        // 因为后面的存储可能与uid相关
        AppContext.uid = accountInfo.uid

        // 记录当前登陆的用户ID
        AppState.user_id = accountInfo.uid

        UserInfo.userAccount = accountInfo
        fetchUserInfo(accountInfo.uid)
    }

    // mock values
    private fun fetchUserInfo(uid: Long) {
        UserInfo.run {
            isVip = true
            gender = Gender.CONVERTER.intToType((uid % 10000 % 3).toInt())
            fansCount = Random.nextInt(10000)
            score = 4.5f
            loginTime = System.currentTimeMillis()
            balance = 99999.99
            sign = "The journey of a thousand miles begins with a single step."
            lock = Utils.getMD5Array("12347".toByteArray())
            tags = setOf("travel", "foods", "cats", randomString())
            favoriteChannels = listOf(1234567, 1234568, 2134569)
            config.putString("theme", "dark")
            config.putBoolean("notification", true)
        }
    }

    private fun randomString(): String {
        val n = Random.nextInt(16)
        val a = ByteArray(n)
        a.fill('a'.code.toByte(), 0, n);
        return String(a)
    }

    fun formatUserInfo(): String {
        val builder = StringBuilder()
        if (isLogin()) {
            UserInfo.run {
                builder
                    .append("gender: ").append(gender).append('\n')
                    .append("isVip: ").append(isVip).append('\n')
                    .append("fansCount: ").append(fansCount).append('\n')
                    .append("score: ").append(score).append('\n')
                    .append("loginTime: ").append(Utils.formatTime(loginTime)).append('\n')
                    .append("balance: ").append(balance).append('\n')
                    .append("sign: ").append(sign).append('\n')
                    .append("lock: ").append(Utils.bytes2Hex(lock)).append('\n')
                    .append("tags: ").append(tags).append('\n')
                    .append("favoriteChannels: ").append(favoriteChannels).append('\n')
                    .append("theme: ").append(config.getString("theme")).append('\n')
                    .append("notification: ").append(config.getBoolean("notification"))
            }
        }
        return builder.toString()
    }
}