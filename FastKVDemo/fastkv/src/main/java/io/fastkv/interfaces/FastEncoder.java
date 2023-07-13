package io.fastkv.interfaces;

import androidx.annotation.NonNull;

public interface FastEncoder<T> {
    String tag();

    byte[] encode(@NonNull T obj);

    T decode(@NonNull byte[] bytes, int offset, int length);
}
