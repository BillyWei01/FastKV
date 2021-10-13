package io.github.fastkvdemo.application

import android.content.Context
import io.github.fastkvdemo.BuildConfig

object GlobalConfig {
    val APPLICATION_ID: String = BuildConfig.APPLICATION_ID

    lateinit var appContext: Context

}