package io.github.fastkvdemo.application;

import android.annotation.SuppressLint;
import android.app.ActivityManager;
import android.app.Application;
import android.content.Context;
import android.os.Build;
import android.text.TextUtils;
import android.util.Log;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Method;

/*
 * From: https://juejin.cn/post/6903867800839897095
 */
public class ProcessUtil {
    private static final String TAG = "ProcessUtil";

    private static String sProcessName = "";

    private ProcessUtil() {
    }

    public static String getProcessName(Context context) {
        if(!TextUtils.isEmpty(sProcessName)) {
            return sProcessName;
        }

        try {
            sProcessName = getCurrentProcessNameByActivityManager(context);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }

        if(!TextUtils.isEmpty(sProcessName)) {
            return sProcessName;
        }

        try {
            sProcessName = getCurrentProcessNameByApplication();
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }

        if(!TextUtils.isEmpty(sProcessName)) {
            return sProcessName;
        }

        try {
            sProcessName = getProcessNameByCmd();
        } catch (Exception e) {
            Log.e(TAG, e.toString(), e);
        }

        if(!TextUtils.isEmpty(sProcessName)) {
            return sProcessName;
        }

        try {
            sProcessName = getCurrentProcessNameByActivityThread();
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }

        return sProcessName;
    }

    public static String getCurrentProcessNameByActivityManager(Context context) {
        int pid = android.os.Process.myPid();
        ActivityManager mActivityManager = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
        for (ActivityManager.RunningAppProcessInfo appProcess : mActivityManager.getRunningAppProcesses()) {
            if (appProcess.pid == pid) {
                return appProcess.processName;
            }
        }
        return null;
    }

    public static String getCurrentProcessNameByApplication() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            return Application.getProcessName();
        }
        return null;
    }

    public static String getProcessNameByCmd() {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/" + android.os.Process.myPid() + "/cmdline"))) {
            String processName = reader.readLine();
            if (!TextUtils.isEmpty(processName)) {
                processName = processName.trim();
            }
            return processName;
        } catch (Exception e) {
            Log.e(TAG, "getProcessName read is fail. exception=" + e);
        }
        return null;
    }

    public static String getCurrentProcessNameByActivityThread() {
        String processName = null;
        try {
            @SuppressLint("PrivateApi")
            final Method declaredMethod = Class.forName("android.app.ActivityThread", false, Application.class.getClassLoader())
                    .getDeclaredMethod("currentProcessName", (Class<?>[]) new Class[0]);
            declaredMethod.setAccessible(true);
            final Object invoke = declaredMethod.invoke(null, new Object[0]);
            if (invoke instanceof String) {
                processName = (String) invoke;
            }
        } catch (Throwable e) {
            Log.e(TAG, e.toString());
        }
        return processName;
    }
}
