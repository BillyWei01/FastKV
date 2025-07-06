package io.fastkv;

import java.util.concurrent.*;

import io.fastkv.interfaces.FastLogger;

public final class FastKVConfig {
    static FastLogger sLogger = null;
    static volatile Executor sExecutor;

    private FastKVConfig() {
    }

    public static void setLogger(FastLogger logger) {
        sLogger = logger;
    }

    /**
     * 强烈建议设置您自己的 Executor 以在公共线程池中重用线程。
     *
     * @param executor 用于加载或写入的执行器。
     */
    public static void setExecutor(Executor executor) {
        if (executor != null) {
            sExecutor = executor;
        }
    }

    static Executor getExecutor() {
        if (sExecutor == null) {
            synchronized (FastKVConfig.class) {
                if (sExecutor == null) {
                    sExecutor = Executors.newCachedThreadPool();
                }
            }
        }
        return sExecutor;
    }
}
