package io.fastkv;

import android.util.Log;

import androidx.annotation.NonNull;

class DefaultLogger implements FastKV.Logger {
    private static final String TAG = "FastKV";

    @Override
    public void i(@NonNull String name, @NonNull String message) {
        Log.i(TAG, name + " " + message);
    }

    @Override
    public void w(@NonNull String name, @NonNull Exception e) {
        Log.w(TAG, name, e);
    }

    @Override
    public void e(@NonNull String name, @NonNull Exception e) {
        Log.e(TAG, name, e);
    }
}
