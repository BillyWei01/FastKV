package io.fastkv.fastkvdemo.event

import android.os.Handler
import android.os.Looper
import android.util.SparseArray
import java.util.*

object EventManager {
    private val handler = Handler(Looper.getMainLooper())
    private val observers = SparseArray<LinkedList<Observer>>(16)

    @Synchronized
    fun register(observer: Observer?) {
        observer?.listenEvents()?.forEach { event ->
            var observerList = observers.get(event)
            if (observerList == null) {
                observerList = LinkedList()
                observerList.add(observer)
                observers.put(event, observerList)
            } else if (observer !in observerList) {
                observerList.add(observer)
            }
        }
    }

    @Synchronized
    fun unregister(observer: Observer?) {
        observer?.listenEvents()?.forEach { event ->
            observers.get(event)?.removeLastOccurrence(observer)
        }
    }

    @Synchronized
    fun notify(event: Int, vararg args: Any?) {
        observers.get(event)?.forEach { observer ->
            handler.post { observer.onEvent(event, *args) }
        }
    }
}