package io.fastkv.fastkvdemo.base

import android.content.Context

interface IAppContext {
    val context: Context
    val debug: Boolean
    val isMainProcess: Boolean
}