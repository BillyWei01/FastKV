package io.fastkv;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

class ExternalExecutor {
    private final Map<String, WrapperTask> nameToTasks = Collections.synchronizedMap(new HashMap<>());

    void execute(String name, CancelableTask task) {
        WrapperTask newTask = wrapTask(name, task);
        nameToTasks.put(name, newTask);
        FastKVConfig.getExecutor().execute(newTask);
    }

    private WrapperTask wrapTask(String name, CancelableTask task) {
        return new WrapperTask(name, task) {
            @Override
            public void run() {
                try {
                    realTask.execute(canceled);
                } finally {
                    nameToTasks.remove(name);
                }
            }
        };
    }

    boolean isNotEmpty() {
        return !nameToTasks.isEmpty();
    }

    /**
     * Try to mark task to be canceled.
     * <p>
     * If there is a task with 'name' in progress, make it canceled an return true,
     * return false otherwise.
     */
    boolean cancelTask(String name) {
        WrapperTask task = nameToTasks.get(name);
        if (task != null) {
            task.canceled.set(true);
            return true;
        }
        return false;
    }

    interface CancelableTask {
        void execute(AtomicBoolean canceled);
    }

    private static abstract class WrapperTask implements Runnable {
        final String name;
        final CancelableTask realTask;
        AtomicBoolean canceled = new AtomicBoolean();

        WrapperTask(String name, CancelableTask task) {
            this.name = name;
            this.realTask = task;
        }
    }
}
