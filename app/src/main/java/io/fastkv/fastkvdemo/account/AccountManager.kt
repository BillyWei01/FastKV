package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.util.Utils
import java.lang.StringBuilder
import kotlin.random.Random

object AccountManager {
    fun isLogin(): Boolean {
        return UserData.userAccount != null
    }

    fun logout() {
        UserData.clear()
    }

    fun login(uid: Long) {
        val account = AccountInfo(uid, "mock token", "hello", "12312345678", "foo@gmail.com")
        UserData.userAccount = account
        fetchUserInfo()
    }

    // mock values
    private fun fetchUserInfo() {
        UserData.run {
            isVip = true
            fansCount = Random.nextInt(10000)
            score = 4.5f
            loginTime = System.currentTimeMillis()
            balance = 99999.99
            sign = "The journey of a thousand miles begins with a single step."
            lock = Utils.md5("12347".toByteArray())
            tags = setOf("travel", "foods", "cats", randomString())
            favoriteChannels = null//listOf(1234567, 1234568, 2134569)
        }
    }

    private fun randomString(): String{
        val n = Random.nextInt(16)
        val a = ByteArray(n)
        a.fill('a'.code.toByte(),0, n);
        return String(a)
    }

    fun formatUserInfo() : String {
        val builder = StringBuilder()
        if (isLogin()) {
            UserData.run {
                builder
                    .append("isVip: ").append(isVip).append('\n')
                    .append("fansCount: ").append(fansCount).append('\n')
                    .append("score: ").append(score).append('\n')
                    .append("loginTime: ").append(Utils.formatTime(loginTime)).append('\n')
                    .append("balance: ").append(balance).append('\n')
                    .append("sign: ").append(sign).append('\n')
                    .append("lock: ").append(Utils.bytes2Hex(lock)).append('\n')
                    .append("tags: ").append(tags).append('\n')
                    .append("favoriteChannels: ").append(favoriteChannels)
            }
        }
        return builder.toString()
    }
}