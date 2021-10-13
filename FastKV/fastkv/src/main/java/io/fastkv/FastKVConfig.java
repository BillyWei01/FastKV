package io.fastkv;

import java.util.concurrent.*;

public final class FastKVConfig {
    static FastKV.Logger sLogger;
    static volatile Executor sExecutor;

    private FastKVConfig() {
    }

    public static void setLogger(FastKV.Logger logger) {
        sLogger = logger;
    }

    /**
     * It's highly recommended to set your own Executor for reusing threads in common thread pool.
     *
     * @param executor The executor for loading or writing.
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
                    ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4,
                            30, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
                    executor.allowCoreThreadTimeOut(true);
                    sExecutor = executor;
                }
            }
        }
        return sExecutor;
    }
}
