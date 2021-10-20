package io.github.fastkvdemo

import android.content.Context
import android.util.Log
import android.util.Pair
import androidx.datastore.preferences.core.PreferenceDataStoreFactory
import androidx.datastore.preferences.core.booleanPreferencesKey
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.floatPreferencesKey
import androidx.datastore.preferences.core.intPreferencesKey
import androidx.datastore.preferences.core.longPreferencesKey
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.core.stringSetPreferencesKey
import com.tencent.mmkv.MMKV
import io.fastkv.FastKV
import io.github.fastkvdemo.application.GlobalConfig
import io.github.fastkvdemo.manager.PathManager
import io.github.fastkvdemo.manager.PathManager.fastKVDir
import io.github.fastkvdemo.util.IOUtil
import io.github.fastkvdemo.util.Utils
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import java.io.File
import java.io.IOException
import java.util.*

object BenchMark {
    private const val TAG = "Benchmark"
    private val sp = GlobalConfig.appContext.getSharedPreferences("sp", Context.MODE_PRIVATE)
    private val dataStore =
        PreferenceDataStoreFactory.create {
            File(
                PathManager.filesDir,
                "data_store.preferences_pb"
            )
        }
    private val mmkv = MMKV.defaultMMKV()
    private val fastkv = FastKV.Builder(fastKVDir, "fastkv").build()

    private const val MILLION = 1000000
    private var hadWarmingUp = false
    suspend fun start() {
        try {
            runTest(GlobalConfig.appContext)
        } catch (e: Throwable) {
            Log.e(TAG, e.message, e)
        }
    }

    @Throws(InterruptedException::class, IOException::class)
    private suspend fun runTest(context: Context) {
        val r = Random(1)
        val srcList = generateInputList(loadSourceData(context))
        warmingUp(srcList, r)
        var inputList: List<Pair<String, Any>>
        Log.i(TAG, "Clear data")
        sp.edit().clear().apply()
        dataStore.edit { it.clear() }
        mmkv.clear()
        fastkv.clear()
        Log.i(TAG, "Start test")
        val time = LongArray(4)
        Arrays.fill(time, 0L)
        inputList = ArrayList(srcList)
        for (i in 0..0) {
            val t1 = System.nanoTime()
            applyToSp(inputList)
            val t2 = System.nanoTime()
            applyToDataStore(inputList)
            val t3 = System.nanoTime()
            putToMMKV(inputList)
            val t4 = System.nanoTime()
            putToFastKV(inputList)
            val t5 = System.nanoTime()
            time[0] += t2 - t1
            time[1] += t3 - t2
            time[2] += t4 - t3
            time[3] += t5 - t4
        }
        Log.i(
            TAG,
            "Fill, sp: " + time[0] / MILLION + ", dataStore: " + time[1] / MILLION
                    + ", mmkv: " + time[2] / MILLION + ", fastkv:" + time[3] / MILLION
        )
        Arrays.fill(time, 0L)
        var round = 10
        for (i in 0 until round) {
            inputList = getDistributedList(srcList, r)
            val t1 = System.nanoTime()
            applyToSp(inputList)
            val t2 = System.nanoTime()
            // TODO DataStore writes too slow, you could exclude this to speed up other test.
            applyToDataStore(inputList)
            val t3 = System.nanoTime()
            putToMMKV(inputList)
            val t4 = System.nanoTime()
            putToFastKV(inputList)
            val t5 = System.nanoTime()
            val a = t2 - t1
            val b = t3 - t2
            val c = t4 - t3
            val d = t5 - t4
            time[0] += a
            time[1] += b
            time[2] += c
            time[3] += d
            Log.d(
                TAG, "Update, sp: " + a / MILLION + ", dataStore: " + b / MILLION
                        + ", mmkv: " + c / MILLION + ", fastkv:" + d / MILLION
            )
        }
        Log.i(
            TAG, "Update total time, sp: " + time[0] / MILLION + ", dataStore: " + time[1] / MILLION
                    + ", mmkv: " + time[2] / MILLION + ", fastkv:" + time[3] / MILLION
        )

        round = 10
        Arrays.fill(time, 0L)
        for (i in 0 until round) {
            val t1 = System.nanoTime()
            readFromSp(inputList)
            val t2 = System.nanoTime()
            readFromDataStore(inputList)
            val t3 = System.nanoTime()
            readFromMMKV(inputList)
            val t4 = System.nanoTime()
            readFromFastKV(inputList)
            val t5 = System.nanoTime()
            time[0] += t2 - t1
            time[1] += t3 - t2
            time[2] += t4 - t3
            time[3] += t5 - t4
        }
        Log.i(
            TAG, "Read total time, sp: " + time[0] / MILLION + ", dataStore: " + time[1] / MILLION
                    + ", mmkv: " + time[2] / MILLION + ", fastkv:" + time[3] / MILLION
        )
    }

    @Throws(InterruptedException::class)
    private suspend fun warmingUp(srcList: ArrayList<Pair<String, Any>>, r: Random) {
        if (!hadWarmingUp) {
            applyToSp(srcList)
            applyToDataStore(srcList)
            putToMMKV(srcList)
            putToFastKV(srcList)

            for (i in 0 until 2) {
                val inputList = getDistributedList(srcList, r)
                applyToSp(inputList)
                // applyToDataStore take too much time
                // applyToDataStore(inputList)
                putToMMKV(inputList)
                putToFastKV(inputList)
            }
            hadWarmingUp = true
            delay(50L)
        }
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
        val a = Utils.getDistributedArray(srcList.size, 2, r)
        for (index in a) {
            val pair = srcList[index]
            inputList.add(Pair(pair.first, tuningObject(pair.second, r)))
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
        val newLen = Math.max(0, len + diff)
        val newStr: String
        newStr = if (newLen < len) {
            str.substring(0, newLen)
        } else if (newLen > len) {
            str + generateString(newLen - len)
        } else {
            if (str.isEmpty()) "" else str.substring(0, len - 1) + "a"
        }
        return newStr
    }

    private fun tuningObject(value: Any, r: Random): Any {
        val diff = 2 - r.nextInt(5)
        return if (value is String) {
            makeNewString(value, diff)
        } else if (value is Boolean) {
            diff < 0
        } else if (value is Int) {
            value + diff
        } else if (value is Long) {
            value + diff
        } else if (value is Float) {
            value + diff
        } else if (value is Set<*>) {
            val oldValue = value as Set<String>
            val newValue: MutableSet<String> = LinkedHashSet()
            for (str in oldValue) {
                newValue.add(makeNewString(str, diff))
            }
            newValue
        } else {
            value
        }
    }

    private fun generateInputList(all: Map<String, *>): ArrayList<Pair<String, Any>> {
        val list = ArrayList<Pair<String, Any>>(all.size)
        for ((key, value) in all) {
            list.add(Pair(key, value))
        }
        return list
    }

    private fun applyToSp(list: List<Pair<String, Any>>) {
        val editor = sp.edit()
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

    private fun readFromSp(list: List<Pair<String, Any>>) {
        for (pair in list) {
            val key = pair.first
            val value = pair.second
            if (value is String) {
                sp.getString(key, "")
            } else if (value is Boolean) {
                sp.getBoolean(key, false)
            } else if (value is Int) {
                sp.getInt(key, 0)
            } else if (value is Long) {
                sp.getLong(key, 0L)
            } else if (value is Float) {
                sp.getFloat(key, 0f)
            } else if (value is Set<*>) {
                sp.getStringSet(key, null)
            }
        }
    }

    private suspend fun applyToDataStore(list: List<Pair<String, Any>>) {
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
        val value = dataStore.data.map { setting ->
            for (pair in list) {
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
                }
            }
        }
        val v = value.first()
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
}