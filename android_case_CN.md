## 1. 兼容SharePreferences

SP支持getAll接口，而FastKV支持putAll接口，所以导入SP数据到FastKV很方便。

```java
public class CommonStore {
    public static final String NAME = "common_store";
    // 原本的获取SP的方法
    // public static final SharedPreferences preferences = GlobalConfig.appContext.getSharedPreferences(NAME, Context.MODE_PRIVATE);

    // 导入原SP数据
    public static final SharedPreferences preferences = FastPreferences.adapt(GlobalConfig.appContext, NAME, false);
}
```

FastPreferences的实现大概如下：

```java
public class FastPreferences implements SharedPreferences {
    private static final String IMPORT_FLAG = "import_flag";

    protected final FastKV kv;
    protected final FastEditor editor = new FastEditor();

    public FastPreferences(String path, String name) {
        kv = new FastKV.Builder(path, name).build();
    }

    public static SharedPreferences adapt(Context context, String name, boolean deleteOldData) {
        String path = context.getFilesDir().getAbsolutePath() + "/fastkv";
        FastPreferences newPreferences = new FastPreferences(path, name);
        if (!newPreferences.contains(IMPORT_FLAG)) {
            SharedPreferences oldPreferences = context.getSharedPreferences(name, Context.MODE_PRIVATE);
            //noinspection unchecked
            Map<String, Object> allData = (Map<String, Object>) oldPreferences.getAll();
            FastKV kv = newPreferences.getKV();
            kv.putAll(allData);
            kv.putBoolean(IMPORT_FLAG, true);
            if (deleteOldData) {
                oldPreferences.edit().clear().apply();
            }
        }
        return newPreferences;
    }
    
    private class FastEditor implements SharedPreferences.Editor {
        @Override
        public Editor putString(String key, @Nullable String value) {
            kv.putString(key, value);
            return this;
        }
    }
    // ...
}
```

FastPreferences是SharedPreferences的实现类，其内部用FastKV存储key-value。
用FastPreferences替换SDK的SharedPreferencesImpl（由context.getSharedPreferences返回)之后，由于接口不变，不需要改动其他代码。

## 2. Kotlin下的用法

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
