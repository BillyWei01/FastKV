package io.fastkv.interfaces;

import androidx.annotation.NonNull;

public interface FastCipher {
  byte[] encrypt(@NonNull byte[] src);

  byte[] decrypt(@NonNull byte[] dst);

  int encrypt(int src);

  int decrypt(int dst);

  long encrypt(long src);

  long decrypt(long dst);
}
