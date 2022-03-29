## 1. 兼容SharePreferences

SP支持getAll接口，而FastKV支持putAll接口，所以导入SP数据到FastKV很方便。

```java
public class CommonStore {
    public static final String NAME = "common_store";
    // 原本的获取SP的方法
    // public static final SharedPreferences preferences = GlobalConfig.appContext.getSharedPreferences(NAME, Context.MODE_PRIVATE);

    // 导入原SP数据
    public static final SharedPreferences preferences = FastPreferences.adapt(GlobalConfig.appContext, NAME);
}
```

FastPreferences的代码实现：[FastPreferences](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/fastkv/src/main/java/io/fastkv/FastPreferences.java) <br>
FastPreferences是SharedPreferences的实现类，用FastPreferences替换SDK的SharedPreferencesImpl（由context.getSharedPreferences返回)之后，由于接口不变，不需要改动其他代码。<br>

## 2. 多进程
项目提供了支持多进程的实现：[MPFastKV](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/fastkv/src/main/java/io/fastkv/MPFastKV.java)。<br>
MPFastKV除了支持多进程读写之外，还实现了SharedPreferences的接口，包括支持注册OnSharedPreferenceChangeListener;<br>
其中一个进程修改了数据，所有的进程都会感知（通过OnSharedPreferenceChangeListener回调）。<br>
具体用法可参考 [MultiProcessTestActivity](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/app/src/main/java/io/fastkv/fastkvdemo/MultiProcessTestActivity.kt) 和 [TestService](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/app/src/main/java/io/fastkv/fastkvdemo/TestService.kt)

需要提醒的是，由于支持多进程需要维护更多的状态 [MPFastKV](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/fastkv/src/main/java/io/fastkv/MPFastKV.java) 的写入要比 [FastKV](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/fastkv/src/main/java/io/fastkv/FastKV.java) 慢不少，所以在不需要多进程访问的情况下，尽量用 [FastKV](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/fastkv/src/main/java/io/fastkv/FastKV.java) 或 [FastPreferences](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/fastkv/src/main/java/io/fastkv/FastPreferences.java)。


## 3. Kotlin API
Kotlin是兼容Java的，所以Kotlin下也可以直接用FastKV或者SharedPreferences的API。
此外，Kotlin还提供了“委托属性”这一语法糖，可以用于改进key-value API访问。

示例用法如下：

定义：
```kotlin
object UserData: KVData("user_data") {
    override fun encoders(): Array<FastKV.Encoder<*>> {
        return arrayOf(AccountInfo.ENCODER, LongListEncoder)
    }

    var userAccount by obj("user_account", AccountInfo.ENCODER)
    var isVip by boolean("is_vip")
    var fansCount by int("fans_count")
    var score by float("score")
    var loginTime by long("login_time")
    var balance by double("balance")
    var sign by string("sing")
    var lock by array("lock")
    var tags by stringSet("tags")
    var favoriteChannels by obj("favorite_channels", LongListEncoder)
}
```

访问：
```kotlin
fun login(uid: Long) {
    val account = AccountInfo(uid, "mock token", "hello", "12312345678", "foo@gmail.com")
    UserData.userAccount = account
    fetchUserInfo()
}

fun fetchUserInfo() {
    UserData.run {
        isVip = true
        fansCount = 99
        score = 4.5f
        loginTime = System.currentTimeMillis()
        balance = 99999.99
        sign = "The journey of a thousand miles begins with a single step."
        lock = Utils.md5("12347".toByteArray())
        tags = setOf("travel", "foods", "cats")
        favoriteChannels = listOf(1234567, 1234568, 2134569)
    }
}
```

其中KVData的实现如下：

```kotlin
open class KVData(name: String) {
    val kv: FastKV by lazy {
        FastKV.Builder(PathManager.fastKVDir, name).encoder(encoders()).build()
    }

    protected open fun encoders(): Array<FastKV.Encoder<*>>? {
        return null
    }

    protected fun boolean(key: String, defValue: Boolean = false) = BooleanProperty(key, defValue)
    protected fun int(key: String, defValue: Int = 0) = IntProperty(key, defValue)
    protected fun float(key: String, defValue: Float = 0f) = FloatProperty(key, defValue)
    protected fun long(key: String, defValue: Long = 0L) = LongProperty(key, defValue)
    protected fun double(key: String, defValue: Double = 0.0) = DoubleProperty(key, defValue)
    protected fun string(key: String, defValue: String = "") = StringProperty(key, defValue)
    protected fun array(key: String, defValue: ByteArray = EMPTY_ARRAY) = ArrayProperty(key, defValue)
    protected fun stringSet(key: String, defValue: Set<String>? = null) = StringSetProperty(key, defValue)
    protected fun <T> obj(key: String, encoder: FastKV.Encoder<T>) = ObjectProperty(key, encoder)

    companion object {
        val EMPTY_ARRAY = ByteArray(0)
    }
}

class BooleanProperty(private val key: String, private val defValue: Boolean) :
    ReadWriteProperty<KVData, Boolean> {
    override fun getValue(thisRef: KVData, property: KProperty<*>): Boolean {
        return thisRef.kv.getBoolean(key, defValue)
    }

    override fun setValue(thisRef: KVData, property: KProperty<*>, value: Boolean) {
        thisRef.kv.putBoolean(key, value)
    }
}
// ...省略其他实现...
```

KVData未包含在maven发布的jar包中，这里主要是用法展示。<br>
感兴趣的朋友直接copy项目中的代码即可。<br>
代码链接：[KVData](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/app/src/main/java/io/fastkv/fastkvdemo/fastkv/KVData.kt)







