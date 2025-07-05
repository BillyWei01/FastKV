package io.fastkv;

import java.util.concurrent.*;

import io.fastkv.interfaces.FastLogger;

public final class FastKVConfig {
    static FastLogger sLogger = new DefaultLogger();
    static volatile Executor sExecutor;
    static int internalLimit = 4096;

    private FastKVConfig() {
    }

    public static void setInternalLimit(int limit) {
        if (limit >= 2048 && limit <= 0xFFFF) {
            internalLimit = limit;
        }
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
