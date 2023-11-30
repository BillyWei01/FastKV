package io.fastkv;

import io.fastkv.interfaces.FastLogger;
import java.util.concurrent.*;

public final class FastKVConfig {
  static FastLogger sLogger = new DefaultLogger();
  static volatile IExecutor sExecutor;
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
  public static void setExecutor(IExecutor executor) {
    if (executor != null) {
      sExecutor = executor;
    }
  }

  static IExecutor getExecutor() {
    if (sExecutor == null) {
      synchronized (FastKVConfig.class) {
        if (sExecutor == null) {
          sExecutor =
              new IExecutor() {
                final Executor mExecutor = Executors.newCachedThreadPool();

                @Override
                public void execute(Runnable runnable) {
                  mExecutor.execute(runnable);
                }
              };
        }
      }
    }
    return sExecutor;
  }

  public interface IExecutor {
    void execute(Runnable runnable);
  }
}
