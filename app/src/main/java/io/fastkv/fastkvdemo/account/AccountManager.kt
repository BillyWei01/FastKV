package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.util.Utils
import io.fastkv.fastkvdemo.data.AppState
import io.fastkv.fastkvdemo.data.UsageData
import io.fastkv.fastkvdemo.data.UserInfo
import java.lang.StringBuilder
import kotlin.random.Random

object AccountManager {
    fun isLogin(): Boolean {
        return AppContext.uid != 0L
    }

    fun logout(): Boolean {
        // close是可选项：
        // close的话能释放一些内存，
        // 不close的话能加速下一次登录。
        // UserInfo.close(AppContext.uid)

        AppContext.uid = 0L
        AppState.userId = 0L
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
        val uid = accountInfo.uid
        // 首先第一件事情就是切换上下AppContext中的uid,
        // 因为后面的存储和逻辑可能与uid相关
        AppContext.uid = uid

        // 保存用户ID
        AppState.userId = uid

        // 记录当前登录ID，以便在登出后再登录时记忆上次登录的账号
        UsageData.lastLoginUid = uid

        // 写入信息，要直接获取uid对应的实例来写入
        // 避免用户ID切换后，异步任务写错到切换后的实例中（串数据）
        UserInfo.get(uid).userAccount = accountInfo

        fetchUserInfo(uid)
    }

    // mock values
    private fun fetchUserInfo(uid: Long) {
        UserInfo.get(uid).run {
            isVip = true
            gender = Gender.CONVERTER.intToType((uid % 10000 % 3).toInt())
            fansCount = Random.nextInt(10000)
            score = 4.5f
            loginTime = System.currentTimeMillis()
            balance = 99999.99
            sign = "The journey of a thousand miles begins with a single step."
            lock = Utils.getMD5Array("12347".toByteArray())
            tags = setOf("travel", "foods", "cats", randomString())
            friendIdList = listOf(1234567, 1234568, 2134569)
            favorites["Android"] = setOf("A", "B", "C")
            favorites["iOS"] = setOf("D", "E", "F", "G")
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
            UserInfo.get().run {
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
                    .append("friendIdList: ").append(friendIdList).append('\n')
                    .append("favorite, Android:").append(favorites["Android"]).append('\n')
                    .append("favorite, iOS:").append(favorites["iOS"]).append('\n')
                    .append("favorite, PC:").append(favorites["PC"]).append('\n')
                    .append("theme: ").append(config.getString("theme")).append('\n')
                    .append("notification: ").append(config.getBoolean("notification"))
            }
        }
        return builder.toString()
    }
}