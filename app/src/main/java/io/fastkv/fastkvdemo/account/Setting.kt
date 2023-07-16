package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.fastkv.KVData

object Setting: KVData("setting")  {
    const val USE_KEY_STORE = "use_key_store"

    var useKeyStore by boolean(USE_KEY_STORE)
}