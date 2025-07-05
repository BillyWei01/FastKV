package io.fastkv;


import androidx.annotation.NonNull;

import java.util.concurrent.Executor;

/**
 * 用于重复任务。
 * 最多只能有一个任务等待执行。
 */
class LimitExecutor implements Executor {
    private Runnable mActive;
    private Runnable mWaiting;

    public synchronized void execute(@NonNull final Runnable r) {
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
