package io.fastkv.fastkvdemo

import android.content.Context
import android.content.SharedPreferences
import android.util.Log
import android.util.Pair
import androidx.datastore.core.DataStore
import androidx.datastore.preferences.core.PreferenceDataStoreFactory
import androidx.datastore.preferences.core.Preferences
import androidx.datastore.preferences.core.booleanPreferencesKey
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.floatPreferencesKey
import androidx.datastore.preferences.core.intPreferencesKey
import androidx.datastore.preferences.core.longPreferencesKey
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.core.stringSetPreferencesKey
import com.tencent.mmkv.MMKV
import io.fastkv.FastKV
import io.fastkv.fastkvdemo.base.AppContext
import io.fastkv.fastkvdemo.data.UsageData
import io.fastkv.fastkvdemo.manager.PathManager
import io.fastkv.fastkvdemo.sqlitekv.SQLiteKV
import io.fastkv.fastkvdemo.util.IOUtil
import io.fastkv.fastkvdemo.util.Utils
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import java.io.File
import java.io.IOException
import java.lang.StringBuilder
import java.util.Arrays
import java.util.Random

/**
 * 对比
 * SharePreferences-commit
 * DataStore
 * SQLite
 * fastkv-commit
 * SharePreferences-apply
 * MMKV
 * fastkv-mmap
 *
 */
object Benchmark {
    private const val TAG = "Benchmark"

    private const val PREFIX_SP_COMMIT = "sp_commit_"
    private const val PREFIX_SP_APPLY = "sp_apply_"
    private const val PREFIX_DATASTORE = "data_store_"
    private const val PREFIX_SQLITE = "sqlite_"
    private const val PREFIX_MMKV = "mmkv_"
    private const val PREFIX_FASTKV = "fastkv_"

    private lateinit var spCommit: SharedPreferences
    private lateinit var spApply: SharedPreferences
    private lateinit var dataStore: DataStore<Preferences>
    private lateinit var sqliteKV: SQLiteKV
    private lateinit var mmkv: MMKV
    private lateinit var fastkv: FastKV

    private const val MILLION = 1000000L
    private var hadWarmingUp = false

    suspend fun start(progress: (kvCount: Int) -> Unit) {
        try {
            var count = UsageData.benchmarkCount

            if (!hadWarmingUp) {
                initFiles(count)
                // 做一次 warming up, 减少JIT因素的干扰
                warmingUp()
                hadWarmingUp = true
            }
            deleteOldFile(count)

            count++
            UsageData.benchmarkCount = count
            initFiles(count)

            Log.i(TAG, "Start test, SpCommit | DataStore | SQLite | SpApply | MMKV | FastKV"  )
            val kvCountArray = arrayOf(25, 50, 100, 200, 400, 600)
            kvCountArray.forEach { kvCount ->
                progress(kvCount)
                runTest(AppContext.context, kvCount, 3)
            }
            Log.i(TAG, " ")
            progress(-1)
        } catch (e: Throwable) {
            Log.e(TAG, e.message, e)
        }
    }

    private fun initFiles(count: Int) {
        spCommit = AppContext.context.getSharedPreferences("$PREFIX_SP_COMMIT$count", Context.MODE_PRIVATE)
        dataStore = PreferenceDataStoreFactory.create {
            File(PathManager.filesDir, "$PREFIX_DATASTORE$count.preferences_pb")
        }
        sqliteKV = SQLiteKV.Builder("$PREFIX_SQLITE$count").build()
        spApply = AppContext.context.getSharedPreferences("$PREFIX_SP_APPLY$count", Context.MODE_PRIVATE)
        mmkv = MMKV.mmkvWithID("$PREFIX_MMKV$count")
        fastkv = FastKV.Builder(PathManager.fastKVDir, "$PREFIX_FASTKV$count").build()
    }

    // 测试加载时间 (先写入600个key-value，修改代码，重启AP执行此方法）
    private suspend fun testLoading(count: Int) {
        val t0 = System.nanoTime()
        spCommit = AppContext.context.getSharedPreferences("$PREFIX_SP_COMMIT$count", Context.MODE_PRIVATE)
        // 因为有的KV的数据加载是异步的，所以实例化本身不耗时；
        // 数据加载期间会加锁，如果此间有其他线程访问put/get接口，会阻塞，直到数据加载完成。
        // 所以这里查询一个value, 等查询完成，就是基本数据加载完成的时间。
        spCommit.getInt("", 0)
        val t1 = System.nanoTime()

        // apply模式和commit模式只影响更新，数据加载是一样的
//        spApply = AppContext.context.getSharedPreferences("$PREFIX_SP_APPLY$count", Context.MODE_PRIVATE)
//        spApply.getInt("", 0)

        dataStore = PreferenceDataStoreFactory.create {
            File(PathManager.filesDir, "$PREFIX_DATASTORE$count.preferences_pb")
        }
//        val data = runBlocking {
//            dataStore.data.first()
//        }
//        val value = data[stringPreferencesKey("")]
        val flow = dataStore.data.map { setting ->
            setting[stringPreferencesKey("")]
        }
        val value = flow.first()

        val t2 = System.nanoTime()
        sqliteKV = SQLiteKV.Builder("$PREFIX_SQLITE$count").build()
        // sqlite不是异步加载，所以不查询也没关系
        // sqliteKV.getInt("", 0)

        val t3 = System.nanoTime()

        mmkv = MMKV.mmkvWithID("$PREFIX_MMKV$count")
        mmkv.getInt("", 0)

        val t4 = System.nanoTime()

        fastkv = FastKV.Builder(PathManager.fastKVDir, "$PREFIX_FASTKV$count").build()
        fastkv.getInt("", 0)

        val t5 = System.nanoTime()

        val msg = StringBuilder()
            .append(" SharePreferences:").append((t1-t0) / MILLION).append(" ms, ")
            .append(" DataStore:").append((t2-t1)  / MILLION).append(" ms, ")
            .append(" SQLite:").append((t3-t2)  / MILLION).append(" ms, ")
            .append(" MMKV:").append((t4-t3)  / MILLION).append(" ms, ")
            .append(" FastKV:").append((t5-t4)  / MILLION).append(" ms")
            .toString()
        Log.i(TAG, "init time: $msg")

        // key-value 数量 600， P30 Pro 测试结果
        // init time:  Sp-commit:24 ms,  DataStore:109 ms,  SQLite:6 ms,  Sp-apply:10 ms,  MMKV:0 ms,  FastKV:6 ms
    }

    private suspend fun runTest(context: Context, kvCount: Int, testRound: Int) {
        val srcList = generateInputList(loadSourceData(context), kvCount)

        val inputList: List<Pair<String, Any>> = ArrayList(srcList)

        // Log an empty line
        Log.d(TAG, " ")

        val time = LongArray(6)
        Arrays.fill(time, 0L)

        putToSpCommit(inputList)
        putToDataStore(inputList)
        putToSqlite(inputList)
        putToSpApply(inputList)
        putToMMKV(inputList)
        putToFastKV(inputList)

        Arrays.fill(time, 0L)

        val testArray = ArrayList<List<Pair<String, Any>>>()

        val r = Random(0)
        for (i in 0 until testRound) {
            testArray.add(getDistributedList(srcList, r))
        }

        nap()

        var i = 0
        time[i++] = test(testRound) { putToSpCommit(testArray[it]) }
        time[i++] = test(testRound) { putToDataStore(testArray[it]) }
        time[i++] = test(testRound) { putToSqlite(testArray[it]) }
        time[i++] = test(testRound) { putToSpApply(testArray[it]) }
        time[i++] = test(testRound) { putToMMKV(testArray[it]) }
        time[i] = test(testRound) { putToFastKV(testArray[it]) }

        Log.i(TAG, "Write, count: $kvCount, " + getTimeLog(time))

        Arrays.fill(time, 0L)

        i = 0
        time[i++] = test(testRound) { readFromSpCommit(testArray[it]) }
        time[i++] = test(testRound) { readFromDataStore(testArray[it]) }
        time[i++] = test(testRound) { readFromSqlite(testArray[it]) }
        time[i++] = test(testRound) { readFromSpApply(testArray[it]) }
        time[i++] = test(testRound) { readFromMMKV(testArray[it]) }
        time[i] = test(testRound) { readFromFastKV(testArray[it]) }

        Log.i(TAG, "Read, count: $kvCount, " + getTimeLog(time))
    }

    /**
     *  获取原始 key-value
     *  @param all 目前这个"all"有685个key-value
     *  @param kvCount 控制input的key-value的数量
     */
    private fun generateInputList(all: Map<String, *>, kvCount: Int): ArrayList<Pair<String, Any>> {
        val list = ArrayList<Pair<String, Any>>(all.size)
        var count = 0;
        for ((key, value) in all) {
            count++
            if (count > kvCount) break
            list.add(Pair(key, value))
        }
        return list
    }

    private fun getTimeLog(time: LongArray): String {
        var i = 0
        return StringBuilder()
            .append(" Sp-commit:").append(time[i++] / MILLION).append(" ms, ")
            .append(" DataStore:").append(time[i++] / MILLION).append(" ms, ")
            .append(" SQLite:").append(time[i++] / MILLION).append(" ms, ")
            .append(" Sp-apply:").append(time[i++] / MILLION).append(" ms, ")
            .append(" MMKV:").append(time[i++] / MILLION).append(" ms, ")
            .append(" FastKV:").append(time[i] / MILLION).append(" ms")
            .toString()

//        return StringBuilder()
//            .append(" | ").append(time[i++] / MILLION)
//            .append(" | ").append(time[i++] / MILLION)
//            .append(" | ").append(time[i++] / MILLION)
//            .append(" | ").append(time[i++] / MILLION)
//            .append(" | ").append(time[i++] / MILLION)
//            .append(" | ").append(time[i] / MILLION)
//            .toString()
    }

    private suspend fun test(round: Int, block: suspend (i: Int) -> Unit): Long {
        var sum = 0L
        for (i in 0 until round) {
            val t1 = System.nanoTime()
            block(i)
            val t2 = System.nanoTime()
            sum += (t2 - t1)
        }
        nap()
        return sum
    }

    private suspend fun nap() {
        System.gc()
        delay(300L)
    }

    private suspend fun warmingUp() {
        val r = Random(1)
        val srcList = generateInputList(loadSourceData(AppContext.context), 600)
        putToSpCommit(srcList)
        putToDataStore(srcList)
        putToSqlite(srcList)
        putToSpApply(srcList)
        putToMMKV(srcList)
        putToFastKV(srcList)

        for (i in 0 until 3) {
            val inputList = getDistributedList(srcList, r)
            putToSpCommit(srcList)
            putToDataStore(inputList)
            putToSqlite(inputList)
            putToSpApply(inputList)
            putToMMKV(inputList)
            putToFastKV(inputList)
        }

        nap()
    }

    @Throws(IOException::class)
    private fun loadSourceData(context: Context): Map<String, *> {
        val srcName = "sum"
        val spDir = File(context.filesDir.parent, "/shared_prefs")
        val sumFile = File(spDir, "$srcName.xml")
        if (!sumFile.exists()) {
            val bytes = IOUtil.streamToBytes(context.assets.open("$srcName.xml"))
            IOUtil.bytesToFile(bytes, sumFile)
        }
        val sumPreferences = context.getSharedPreferences(srcName, Context.MODE_PRIVATE)
        return sumPreferences.all
    }

    private fun getDistributedList(
        srcList: List<Pair<String, Any>>,
        r: Random
    ): List<Pair<String, Any>> {
        val inputList: MutableList<Pair<String, Any>> = ArrayList(srcList.size)
        val a = Utils.getDistributedArray(srcList.size, 3, r)
        for (index in a) {
            val pair = srcList[index]
            inputList.add(Pair(pair.first, tuningValue(pair.second, r)))
        }
        return inputList
    }

    private fun generateString(size: Int): String {
        val a = CharArray(size)
        for (i in 0 until size) {
            a[i] = ('A'.toInt() + i % 26).toChar()
        }
        return String(a)
    }

    private fun makeNewString(str: String, diff: Int): String {
        val len = str.length
        val newLen = 0.coerceAtLeast(len + diff)
        val newStr: String = if (newLen < len) {
            str.substring(0, newLen)
        } else if (newLen > len) {
            str + generateString(newLen - len)
        } else {
            if (str.isEmpty()) "" else str.substring(0, len - 1) + "a"
        }
        return newStr
    }

    // 对value进行微调
    private fun tuningValue(value: Any, r: Random): Any {
        val diff = 1 - r.nextInt(3)
        return when (value) {
            is String -> {
                makeNewString(value, diff)
            }
            is Boolean -> {
                diff < 0
            }
            is Int -> {
                value + diff
            }
            is Long -> {
                value + diff
            }
            is Float -> {
                value + diff
            }
            is Set<*> -> {
                val oldValue = value as Set<String>
                val newValue: MutableSet<String> = LinkedHashSet()
                for (str in oldValue) {
                    newValue.add(makeNewString(str, diff))
                }
                newValue
            }
            else -> {
                value
            }
        }
    }

    private fun putToSpCommit(list: List<Pair<String, Any>>) {
        val editor = spCommit.edit()
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                editor.putString(key, value).commit()
            } else if (value is Boolean) {
                editor.putBoolean(key, value).commit()
            } else if (value is Int) {
                editor.putInt(key, value).commit()
            } else if (value is Long) {
                editor.putLong(key, value).commit()
            } else if (value is Float) {
                editor.putFloat(key, value).commit()
            } else if (value is Set<*>) {
                editor.putStringSet(key, value as Set<String?>).commit()
            }
        }
    }

    private fun readFromSpCommit(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                spCommit.getString(key, "")
            } else if (value is Boolean) {
                spCommit.getBoolean(key, false)
            } else if (value is Int) {
                spCommit.getInt(key, 0)
            } else if (value is Long) {
                spCommit.getLong(key, 0L)
            } else if (value is Float) {
                spCommit.getFloat(key, 0f)
            } else if (value is Set<*>) {
                spCommit.getStringSet(key, null)
            }
        }
    }

    private fun putToSpApply(list: List<Pair<String, Any>>) {
        val editor = spApply.edit()
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                editor.putString(key, value).apply()
            } else if (value is Boolean) {
                editor.putBoolean(key, value).apply()
            } else if (value is Int) {
                editor.putInt(key, value).apply()
            } else if (value is Long) {
                editor.putLong(key, value).apply()
            } else if (value is Float) {
                editor.putFloat(key, value).apply()
            } else if (value is Set<*>) {
                editor.putStringSet(key, value as Set<String?>).apply()
            }
        }
    }

    private fun readFromSpApply(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                spApply.getString(key, "")
            } else if (value is Boolean) {
                spApply.getBoolean(key, false)
            } else if (value is Int) {
                spApply.getInt(key, 0)
            } else if (value is Long) {
                spApply.getLong(key, 0L)
            } else if (value is Float) {
                spApply.getFloat(key, 0f)
            } else if (value is Set<*>) {
                spApply.getStringSet(key, null)
            }
        }
    }

    private suspend fun putToDataStore(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                dataStore.edit { it[stringPreferencesKey(key)] = value }
            } else if (value is Boolean) {
                dataStore.edit { it[booleanPreferencesKey(key)] = value }
            } else if (value is Int) {
                dataStore.edit { it[intPreferencesKey(key)] = value }
            } else if (value is Long) {
                dataStore.edit { it[longPreferencesKey(key)] = value }
            } else if (value is Float) {
                dataStore.edit { it[floatPreferencesKey(key)] = value }
            } else if (value is Set<*>) {
                dataStore.edit { it[stringSetPreferencesKey(key)] = value as Set<String> }
            }
        }
    }

    private suspend fun readFromDataStore(list: List<Pair<String, Any>>) {
        for (pair in list) {
//            val setting = runBlocking {
//                dataStore.data.first()
//            }
//            val key = pair.first
//            val value = pair.second
//            if (value is String) {
//                setting[stringPreferencesKey(key)]
//            } else if (value is Boolean) {
//                setting[booleanPreferencesKey(key)]
//            } else if (value is Int) {
//                setting[intPreferencesKey(key)]
//            } else if (value is Long) {
//                setting[longPreferencesKey(key)]
//            } else if (value is Float) {
//                setting[floatPreferencesKey(key)]
//            } else if (value is Set<*>) {
//                setting[stringSetPreferencesKey(key)]
//            } else {
//                null
//            }

            val flow = dataStore.data.map { setting ->
                val key = pair.first
                val value = pair.second
                if (value is String) {
                    setting[stringPreferencesKey(key)]
                } else if (value is Boolean) {
                    setting[booleanPreferencesKey(key)]
                } else if (value is Int) {
                    setting[intPreferencesKey(key)]
                } else if (value is Long) {
                    setting[longPreferencesKey(key)]
                } else if (value is Float) {
                    setting[floatPreferencesKey(key)]
                } else if (value is Set<*>) {
                    setting[stringSetPreferencesKey(key)]
                } else {
                    null
                }
            }
            val result = flow.first()
        }
    }

    private fun putToSqlite(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                sqliteKV.putString(key, value)
            } else if (value is Boolean) {
                sqliteKV.putBoolean(key, value)
            } else if (value is Int) {
                sqliteKV.putInt(key, value)
            } else if (value is Long) {
                sqliteKV.putLong(key, value)
            } else if (value is Float) {
                sqliteKV.putFloat(key, value)
            } else if (value is Set<*>) {
                sqliteKV.putStringSet(key, value as Set<String?>)
            }
        }
    }

    private fun readFromSqlite(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                sqliteKV.getString(key, "")
            } else if (value is Boolean) {
                sqliteKV.getBoolean(key, false)
            } else if (value is Int) {
                sqliteKV.getInt(key, 0)
            } else if (value is Long) {
                sqliteKV.getLong(key, 0L)
            } else if (value is Float) {
                sqliteKV.getFloat(key)
            } else if (value is Set<*>) {
                sqliteKV.getStringSet(key, null)
            }
        }
    }

    private fun putToMMKV(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                mmkv.putString(key, value)
            } else if (value is Boolean) {
                mmkv.putBoolean(key, value)
            } else if (value is Int) {
                mmkv.putInt(key, value)
            } else if (value is Long) {
                mmkv.putLong(key, value)
            } else if (value is Float) {
                mmkv.putFloat(key, value)
            } else if (value is Set<*>) {
                mmkv.putStringSet(key, value as Set<String?>)
            }
        }
    }

    private fun readFromMMKV(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                mmkv.getString(key, "")
            } else if (value is Boolean) {
                mmkv.getBoolean(key, false)
            } else if (value is Int) {
                mmkv.getInt(key, 0)
            } else if (value is Long) {
                mmkv.getLong(key, 0L)
            } else if (value is Float) {
                mmkv.getFloat(key, 0f)
            } else if (value is Set<*>) {
                mmkv.getStringSet(key, null)
            }
        }
    }

    private fun putToFastKV(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                fastkv.putString(key, value)
            } else if (value is Boolean) {
                fastkv.putBoolean(key, value)
            } else if (value is Int) {
                fastkv.putInt(key, value)
            } else if (value is Long) {
                fastkv.putLong(key, value)
            } else if (value is Float) {
                fastkv.putFloat(key, value)
            } else if (value is Set<*>) {
                fastkv.putStringSet(key, value as Set<String?>)
            }
        }
    }

    private fun readFromFastKV(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                fastkv.getString(key, "")
            } else if (value is Boolean) {
                fastkv.getBoolean(key, false)
            } else if (value is Int) {
                fastkv.getInt(key, 0)
            } else if (value is Long) {
                fastkv.getLong(key, 0L)
            } else if (value is Float) {
                fastkv.getFloat(key, 0f)
            } else if (value is Set<*>) {
                fastkv.getStringSet(key)
            }
        }
    }

    private fun deleteOldFile(count: Int) {
        kotlin.runCatching {
            deleteSP(PREFIX_SP_COMMIT, count)
            deleteSP(PREFIX_SP_APPLY, count)
            deleteDataStore(count)
            deleteMMKV(count)
            deleteDB(count)
            deleteFastKV(count)
        }.onFailure { t ->
            Log.e(TAG, "delete failed", t)
        }
    }

    private fun getPackageDir(): String {
        return AppContext.context.filesDir.parentFile?.absolutePath ?: ""
    }

    private fun deleteDataStore(count: Int) {
        File(PathManager.filesDir, "$PREFIX_DATASTORE$count.preferences_pb").delete()
    }

    private fun deleteMMKV(count: Int) {
        if (this::mmkv.isInitialized) {
            mmkv.close()
        }

        val dir = PathManager.filesDir + "/mmkv"
        val name = "$PREFIX_MMKV$count"

        File(dir, name).delete()
        File(dir, "$name.crc").delete()
    }

    private fun deleteSP(prefix: String, count: Int) {
        File(getPackageDir() + "/shared_prefs", "$prefix$count.xml").delete()
    }

    private fun deleteDB(count: Int) {
        val dir = getPackageDir() + "/databases"
        val name = "$PREFIX_SQLITE$count"
        SQLiteKV.close(name)
        File(dir, "$name.db").delete()
        File(dir, "$name.db-journal").delete()
    }

    private fun deleteFastKV(count: Int) {
        if (this::fastkv.isInitialized) {
            fastkv.close()
        }
        val dir = PathManager.filesDir + "/fastkv"
        val name = "$PREFIX_FASTKV$count"
        File(dir, "$name.kva").delete()
        File(dir, "$name.kvb").delete()
        File(dir, "$name.kvc").delete()
        File(dir, "$name.tmp").delete()
        File(dir, name).delete()
    }
}
