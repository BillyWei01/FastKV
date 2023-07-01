package io.fastkv;

import java.util.concurrent.*;

public final class FastKVConfig {
    static FastKV.Logger sLogger;
    static volatile Executor sExecutor;
    static int internalLimit = 8192;

    private FastKVConfig() {
    }

    public static void setInternalLimit(int limit) {
        if (limit >= 2048 && limit <= 0xFFFF) {
            internalLimit = limit;
        }
    }

    public static void setLogger(FastKV.Logger logger) {
        sLogger = logger;
    }

    /**
     * It's highly recommended setting your own Executor for reusing threads in common thread pool.
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
                            5, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
                    executor.allowCoreThreadTimeOut(true);
                    sExecutor = executor;
                }
            }
        }
        return sExecutor;
    }
}
