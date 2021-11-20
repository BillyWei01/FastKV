package io.fastkv.fastkvdemo.fastkv

import android.util.Log
import io.fastkv.FastKV
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.lang.Exception

object FastKVLogger : FastKV.Logger {
    private const val tag = "FastKV"

    override fun i(name: String, message: String) {
        if(message != "gc finish"){
            Log.i(tag, "$name $message")
        }
    }

    override fun w(name: String, e: Exception) {
        GlobalScope.launch(Dispatchers.Default){
            Log.w(tag, name, e)
        }
    }

    override fun e(name: String, e: Exception) {
        GlobalScope.launch(Dispatchers.Default) {
            Log.e(tag, name, e)
        }
    }
}