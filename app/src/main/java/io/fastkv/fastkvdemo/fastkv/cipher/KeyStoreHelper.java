package io.fastkv.fastkvdemo.fastkv.cipher;

import android.os.Build;
import android.security.keystore.KeyGenParameterSpec;
import android.security.keystore.KeyProperties;

import androidx.annotation.RequiresApi;

import java.security.Key;
import java.security.KeyStore;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;

import io.fastkv.fastkvdemo.fastkv.utils.FastKVLogger;

public class KeyStoreHelper {
    private static final String ALIAS = "KeyStoreHelper";

    /**
     * Get the random bytes as symmetric key with length of 128 bits.
     * The first time it's generating by random,
     * and in later time it will return the bytes which be the same as the first time.
     * So it's suitable to use as symmetric key.
     *
     * <br>
     * HMAC = H(KEY XOR opad, H(KEY XOR ipad, text))
     * <br>
     * The 'KEY' is generated and stored by KeyStore.
     */
    public static byte[] getKey(byte[] seed) {
        try {
            String algorithm = KeyProperties.KEY_ALGORITHM_HMAC_SHA256;
            KeyStore keyStore = KeyStore.getInstance("AndroidKeyStore");
            keyStore.load(null);
            Key key = keyStore.getKey(ALIAS, null);
            if (key== null) {
                // Generate the key and save to KeyStore, next time we can get it from KeyStore.
                KeyGenerator keyGenerator = KeyGenerator.getInstance(algorithm, "AndroidKeyStore");
                keyGenerator.init(new KeyGenParameterSpec.Builder(ALIAS, KeyProperties.PURPOSE_SIGN).build());
                key = keyGenerator.generateKey();
            }
            Mac mac = Mac.getInstance(algorithm);
            mac.init(key);
            return mac.doFinal(seed);
        } catch (Exception e) {
            FastKVLogger.INSTANCE.e("Cipher", e);
        }
        return new byte[32];
    }
}
