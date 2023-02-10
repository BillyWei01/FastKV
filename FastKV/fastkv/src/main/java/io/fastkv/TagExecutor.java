package io.fastkv;

import java.util.*;

class TagExecutor {
    private static final Set<String> scheduledTags = new HashSet<>();
    private static final Map<String, Runnable> waitingTasks = new HashMap<>();

    public synchronized void execute(String tag, Runnable r) {
        if (r == null) {
            return;
        }
        if (!scheduledTags.contains(tag)) {
            scheduledTags.add(tag);
            start(tag, r);
        } else {
            waitingTasks.put(tag, r);
        }
    }

    private void start(String tag, Runnable r) {
        FastKVConfig.getExecutor().execute(() -> {
            try {
                r.run();
            } finally {
                scheduleNext(tag);
            }
        });
    }

    private synchronized void scheduleNext(String tag) {
        Runnable r = waitingTasks.remove(tag);
        if (r != null) {
            start(tag, r);
        } else {
            scheduledTags.remove(tag);
        }
    }
}
