package io.fastkv.fastkvdemo.fastkv;

import android.os.Build;
import android.security.keystore.KeyGenParameterSpec;
import android.security.keystore.KeyProperties;

import androidx.annotation.NonNull;
import androidx.annotation.RequiresApi;

import java.security.Key;
import java.security.KeyStore;
import java.util.Arrays;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;

public class KeyStoreHelper {
    private static final String ALIAS = "KeyStoreHelper";

    /**
     * Get the random bytes as symmetric key with length of 128.
     * The first time it's generating by random,
     * and in later time it will return the bytes which be the same as first time.
     * So it's suitable to use as symmetric key.
     */
    @RequiresApi(api = Build.VERSION_CODES.M)
    public static byte[] getKey(byte[] seed) {
        byte[] digest = hash(seed);
        return digest != null ? paddingKey(digest) : paddingKey(seed);
    }

    /**
     * HMAC = H(KEY XOR opad, H(KEY XOR ipad, text))
     * <br>
     * The 'KEY' is generated and store by KeyStore.
     */
    @RequiresApi(api = Build.VERSION_CODES.M)
    private synchronized static byte[] hash(@NonNull byte[] text) {
        try {
            String algorithm = KeyProperties.KEY_ALGORITHM_HMAC_SHA256;
            Key key;
            KeyStore keyStore = KeyStore.getInstance("AndroidKeyStore");
            keyStore.load(null);

            if (keyStore.containsAlias(ALIAS)) {
                key = keyStore.getKey(ALIAS, null);
            } else {
                // Generate the key and save to KeyStore, next time we could get it from KeyStore.
                KeyGenerator keyGenerator = KeyGenerator.getInstance(algorithm, "AndroidKeyStore");
                keyGenerator.init(new KeyGenParameterSpec.Builder(ALIAS, KeyProperties.PURPOSE_SIGN).build());
                key = keyGenerator.generateKey();
            }
            Mac mac = Mac.getInstance(algorithm);
            mac.init(key);
            return mac.doFinal(text);
        } catch (Exception e) {
            FastKVLogger.INSTANCE.e("Cipher", e);
        }
        return null;
    }

    private static byte[] paddingKey(byte[] key) {
        if (key == null) {
            return new byte[16];
        } else if (key.length > 16) {
            return Arrays.copyOf(key, 16);
        } else if (key.length == 16) {
            return key;
        } else {
            byte[] padding = new byte[16];
            System.arraycopy(key, 0, padding, 0, key.length);
            return padding;
        }
    }
}
