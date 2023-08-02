package io.fastkv.fastkvdemo.data;

import android.content.SharedPreferences;
import android.util.Log;

import io.fastkv.FastKV;
import io.fastkv.fastkvdemo.base.AppContext;

/**
 * SP 迁移 FastKV 的样例
 */
public class SpCase {
    public static final String NAME = "common_store";
    // public static final SharedPreferences preferences = GlobalConfig.appContext.getSharedPreferences(NAME, Context.MODE_PRIVATE);
    public static final SharedPreferences preferences = FastKV.adapt(AppContext.INSTANCE.getContext(), NAME);

    static {
        preferences.registerOnSharedPreferenceChangeListener((sharedPreferences, key) ->
                Log.d("MyTag", "changed key:" + key)
        );
    }

    public static final String LAUNCH_COUNT = "launch_count";
}
