package io.fastkv.interfaces;

import androidx.annotation.NonNull;

public interface FastLogger {
  void i(@NonNull String name, @NonNull String message);

  void w(@NonNull String name, @NonNull Exception e);

  void e(@NonNull String name, @NonNull Exception e);
}
