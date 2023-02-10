package io.fastkv;



import java.util.concurrent.Executor;

/**
 * Use for duplicate tasks.
 * Only one task at most can wait to be executed.
 */
class LimitExecutor implements Executor {
    private Runnable mActive;
    private Runnable mWaiting;

    public synchronized void execute(final Runnable r) {
        if (mActive == null) {
            mActive = wrapTask(r);
            FastKVConfig.getExecutor().execute(mActive);
        } else {
            if (mWaiting == null) {
                mWaiting = wrapTask(r);
            }
        }
    }

    private Runnable wrapTask(Runnable r) {
        return () -> {
            try {
                r.run();
            } finally {
                scheduleNext();
            }
        };
    }

    private synchronized void scheduleNext() {
        mActive = mWaiting;
        mWaiting = null;
        if (mActive != null) {
            FastKVConfig.getExecutor().execute(mActive);
        }
    }
}
