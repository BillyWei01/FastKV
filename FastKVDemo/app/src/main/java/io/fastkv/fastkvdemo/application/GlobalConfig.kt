package io.fastkv.fastkvdemo.application

import android.content.Context
import io.fastkv.fastkvdemo.BuildConfig

object GlobalConfig {
    val APPLICATION_ID: String = BuildConfig.APPLICATION_ID

    lateinit var appContext: Context

}