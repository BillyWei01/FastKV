# FastKV
[![Maven Central](https://img.shields.io/maven-central/v/io.github.billywei01/fastkv)](https://search.maven.org/artifact/io.github.billywei01/fastkv) | [English](README_EN.md) | [架构设计](ARCHITECTURE.md)

## 1. 概述
FastKV是用Java编写的高效可靠的key-value存储库，专为Android平台优化。

###  核心特性

#### 1. 高性能读写
- **二进制编码**：采用紧凑的二进制格式，相比XML等文本编码体积更小
- **增量更新**：记录key-value的精确偏移量，支持原地更新，避免全量重写
- **mmap内存映射**：默认使用mmap技术，写入数据直接映射到内存
- **垃圾回收**：自动清理无效数据，保持存储空间紧凑

#### 2. 多种写入模式
- **NON_BLOCKING**：非阻塞模式，通过mmap直接写入内存，性能最高
- **ASYNC_BLOCKING**：异步阻塞模式，后台线程写入磁盘，类似SharedPreferences.apply()
- **SYNC_BLOCKING**：同步阻塞模式，立即写入磁盘，类似SharedPreferences.commit()

#### 3. 丰富的数据类型支持
- **基础类型**：boolean, int, float, long, double, String
- **字节数组**：byte[]，支持二进制数据存储
- **字符串集合**：Set<String>，完全兼容SharedPreferences
- **自定义对象**：通过FastEncoder接口支持任意对象序列化


#### 4. 数据安全保障
- **双文件备份**：A/B文件互为备份，确保数据不丢失
- **校验和保护**：每次读取都验证数据完整性
- **原子操作**：写入操作具有原子性，避免数据损坏
- **自动降级**：mmap失败时自动切换到阻塞模式

#### 5. 数据加密支持
- **可插拔加密**：支持注入自定义加密实现
- **透明加解密**：加密在数据写入前执行，解密在数据解析时执行
- **加密迁移**：支持从明文到密文的平滑迁移
- **性能优化**：缓存解密后的数据，不影响读取性能

#### 6. 开发友好
- **兼容性好**：实现SharedPreferences接口，便于迁移
- **迁移工具**：提供adapt()方法自动迁移SharedPreferences数据
- **丰富API**：支持批量操作、监听器等
- **类型安全**：编译时类型检查，避免运行时类型错误

#### 7. 稳定可靠
- **容错机制**：基本的错误检测和自动恢复
- **向前兼容**：新版本可读取旧版本数据
- **异常处理**：基本的异常处理和日志记录

#### 8. 轻量高效
- **体积小**：纯Java实现，编译后仅数十KB
- **内存友好**：减少不必要的对象创建
- **启动快**：异步加载，支持多文件并发加载


## 2. 使用方法

### 2.1 导入

```gradle
dependencies {
    implementation 'io.github.billywei01:fastkv:2.6.0'
}
```

### 2.2 初始化
```kotlin
// 可选：设置全局配置
FastKVConfig.setLogger(FastKVLogger)
FastKVConfig.setExecutor(Dispatchers.IO.asExecutor())
```

初始化可以按需设置日志接口和Executor：
- 如果不传入Executor，FastKV会自己构建一个CachedThreadPool
- 如果传入用户自己的Executor，需要确保Executor的并发调度能力

### 2.3 基本用法

```java
// 使用Context构造（推荐）
FastKV kv = new FastKV.Builder(context, "user_data").build();

// 或使用自定义路径
FastKV kv = new FastKV.Builder(path, "user_data").build();

// 基本读写操作
if (!kv.getBoolean("first_launch")) {
    kv.putBoolean("first_launch", true);
}

int count = kv.getInt("launch_count");
kv.putInt("launch_count", count + 1);

// 支持链式调用
kv.putString("user_name", "张三")
  .putInt("user_age", 25)
  .putFloat("user_score", 89.5f);
```

### 2.4 高级配置

```java
FastKV kv = new FastKV.Builder(context, "secure_data")
    .encoder(new FastEncoder[]{CustomObjectEncoder.INSTANCE})  // 自定义编码器
    .cipher(new AESCipher())                                   // 数据加密
    .blocking()                                                // 同步阻塞模式
    .build();
```

### 2.5 存储自定义对象

```java
// 1. 实现FastEncoder接口
public class UserEncoder implements FastEncoder<User> {
    @Override
    public String tag() {
        return "User";
    }
    
    @Override
    public byte[] encode(User user) {
        // 序列化逻辑
        return userToBytes(user);
    }
    
    @Override
    public User decode(byte[] bytes, int offset, int length) {
        // 反序列化逻辑
        return bytesToUser(bytes, offset, length);
    }
}

// 2. 注册编码器并使用
FastEncoder<?>[] encoders = {new UserEncoder()};
FastKV kv = new FastKV.Builder(context, "user_data")
    .encoder(encoders)
    .build();

// 3. 存储和读取对象
User user = new User("张三", 25);
kv.putObject("current_user", user, new UserEncoder());
User savedUser = kv.getObject("current_user");
```

推荐使用[Packable](https://github.com/BillyWei01/Packable)框架进行对象序列化。

### 2.6 数据加密

```java
// 实现FastCipher接口
public class AESCipher implements FastCipher {
    @Override
    public byte[] encrypt(byte[] src) {
        // 加密实现
        return encryptWithAES(src);
    }
    
    @Override
    public byte[] decrypt(byte[] dst) {
        // 解密实现
        return decryptWithAES(dst);
    }
    
    // 其他加密方法...
}

// 使用加密
FastKV kv = new FastKV.Builder(context, "secure_data")
    .cipher(new AESCipher())
    .build();
```

### 2.7 批量操作

```java
// 批量写入
Map<String, Object> data = new HashMap<>();
data.put("name", "张三");
data.put("age", 25);
data.put("score", 89.5f);
data.put("active", true);

kv.putAll(data);

// 批量读取
Map<String, Object> allData = kv.getAll();

// 事务控制（阻塞模式）
kv.disableAutoCommit();
kv.putString("key1", "value1");
kv.putString("key2", "value2");
kv.commit(); // 一次性提交所有更改
```

### 2.8 迁移SharedPreferences

```java
public class SpCase {
    public static final String NAME = "common_store";
    
    // 替换原有的SharedPreferences获取方式
    // public static final SharedPreferences preferences = context.getSharedPreferences(NAME, Context.MODE_PRIVATE);
    
    // 使用FastKV并自动迁移数据
    public static final SharedPreferences preferences = FastKV.adapt(context, NAME);
}
```

### 2.9 监听数据变化

```java
kv.registerOnSharedPreferenceChangeListener(new SharedPreferences.OnSharedPreferenceChangeListener() {
    @Override
    public void onSharedPreferenceChanged(SharedPreferences sharedPreferences, String key) {
        // 处理数据变化
        System.out.println("Key changed: " + key);
    }
});
```

### 2.10 Kotlin委托属性

```kotlin
// 使用委托属性简化访问
class UserSettings(private val kv: FastKV) {
    var userName: String by kv.string("user_name", "")
    var userAge: Int by kv.int("user_age", 0)
    var isVip: Boolean by kv.boolean("is_vip", false)
}

// 使用示例
val settings = UserSettings(kv)
settings.userName = "张三"
settings.userAge = 25
println("用户: ${settings.userName}, 年龄: ${settings.userAge}")
```

### 2.11 注意事项

1. **路径和名称一致性**：不同版本之间不要改变路径和名字，否则会打开不同的文件
2. **加密器一致性**：如果使用了Cipher，不要更换，否则无法解析数据（从无加密到加密是可以的）
3. **类型一致性**：同一个key对应的value类型应保持一致

## 3. 性能测试

### 测试环境
- **测试数据**：600+个真实key-value数据（经过随机混淆）
- **测试设备**：华为P30 Pro
- **测试方法**：正态分布输入序列，多次测试取平均值

### 测试结果

**写入性能（毫秒）**：

| 数据量 | 25 | 50 | 100 | 200 | 400 | 600 |
|--------|----|----|-----|-----|-----|-----|
| SP-commit | 114 | 172 | 411 | 666 | 2556 | 5344 |
| DataStore | 231 | 625 | 1717 | 4421 | 7629 | 13639 |
| SQLiteKV | 192 | 382 | 1025 | 1565 | 4279 | 5034 |
| SP-apply | 3 | 9 | 35 | 118 | 344 | 516 |
| MMKV | 4 | 8 | 5 | 8 | 10 | 9 |
| **FastKV** | **3** | **6** | **4** | **6** | **8** | **10** |

**读取性能（毫秒）**：

| 数据量 | 25 | 50 | 100 | 200 | 400 | 600 |
|--------|----|----|-----|-----|-----|-----|
| SP-commit | 1 | 3 | 2 | 1 | 2 | 3 |
| DataStore | 57 | 76 | 115 | 117 | 170 | 216 |
| SQLiteKV | 96 | 161 | 265 | 417 | 767 | 1038 |
| SP-apply | 0 | 1 | 0 | 1 | 3 | 3 |
| MMKV | 0 | 1 | 1 | 5 | 8 | 11 |
| **FastKV** | **0** | **1** | **1** | **3** | **3** | **1** |

### 性能优势
- **写入速度**：与MMKV相当，比SharedPreferences显著更快
- **读取速度**：与SharedPreferences相当，比DataStore更快
- **启动速度**：异步加载，不阻塞应用启动

## 4. 架构设计

FastKV采用模块化设计，详细的架构说明请参考[ARCHITECTURE.md](ARCHITECTURE.md)。

### 核心模块
- **FastKV**：核心API和业务逻辑
- **FileHelper**：文件I/O和备份管理
- **DataParser**：数据解析和序列化
- **GCHelper**：垃圾回收和内存管理
- **BufferHelper**：缓冲区操作和校验和计算

### 关键特性
- **双文件备份**：A/B文件确保数据安全
- **智能垃圾回收**：自动清理无效数据
- **多种写入模式**：适应不同性能需求
- **可插拔加密**：支持自定义加密算法

## 5. 相关链接

- **技术博客**：https://juejin.cn/post/7018522454171582500
- **纯Java版本**：https://github.com/BillyWei01/FastKV-Java
- **序列化框架**：https://github.com/BillyWei01/Packable
- **示例代码**：[FastKV Demo](app/src/main/java/io/fastkv/fastkvdemo/)

## License
See the [LICENSE](LICENSE) file for license rights and limitations.



