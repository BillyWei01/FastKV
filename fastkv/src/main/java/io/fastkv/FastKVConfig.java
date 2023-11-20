package io.fastkv;

import io.fastkv.interfaces.FastLogger;
import java.util.concurrent.*;

public final class FastKVConfig {
  static FastLogger sLogger = new DefaultLogger();
  static volatile Executor sExecutor;
  static int internalLimit = 4096;

  private FastKVConfig() {}

  public static void setInternalLimit(int limit) {
    if (limit >= 2048 && limit <= 0xFFFF) {
      internalLimit = limit;
    }
  }

  public static void setLogger(FastLogger logger) {
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
          sExecutor = Executors.newCachedThreadPool();
        }
      }
    }
    return sExecutor;
  }
}
