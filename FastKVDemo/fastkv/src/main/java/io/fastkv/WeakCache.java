package io.fastkv;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

final class WeakCache {
    private final Map<String, ValueReference> cache = new HashMap<>();
    private final ReferenceQueue<Object> queue = new ReferenceQueue<>();

    synchronized Object get(String key) {
        cleanQueue();
        ValueReference reference = cache.get(key);
        return reference != null ? reference.get() : null;
    }

    synchronized void put(String key, Object value) {
        cleanQueue();
        if (value != null) {
            ValueReference reference = cache.get(key);
            if (reference == null || reference.get() != value) {
                cache.put(key, new ValueReference(key, value, queue));
            }
        }
    }

    synchronized void remove(String key) {
        cache.remove(key);
    }

    synchronized void clear() {
        cache.clear();
        cleanQueue();
    }

    private void cleanQueue() {
        ValueReference reference = (ValueReference) queue.poll();
        while (reference != null) {
            ValueReference ref = cache.get(reference.key);
            if (ref != null && ref.get() == null) {
                cache.remove(reference.key);
            }
            reference = (ValueReference) queue.poll();
        }
    }

    private static class ValueReference extends WeakReference<Object> {
        private final String key;

        ValueReference(String key, Object value, ReferenceQueue<Object> q) {
            super(value, q);
            this.key = key;
        }
    }
}
