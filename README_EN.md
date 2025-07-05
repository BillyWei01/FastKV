# FastKV 
[![Maven Central](https://img.shields.io/maven-central/v/io.github.billywei01/fastkv)](https://search.maven.org/artifact/io.github.billywei01/fastkv) | [中文文档](README.md) | [Architecture](ARCHITECTURE.md)

## 1. Overview
FastKV is an efficient and reliable key-value storage library written in Java, optimized for Android platforms.

###  Core Features

#### 1. High-Performance Read/Write
- **Binary Encoding**: Compact binary format, smaller than XML and other text encodings
- **Incremental Updates**: Records precise offsets of key-value pairs, supports in-place updates, avoids full rewrites
- **mmap Memory Mapping**: Uses mmap technology by default, writes data directly to memory
- **Garbage Collection**: Automatically cleans up invalid data, maintains compact storage space

#### 2. Multiple Writing Modes
- **NON_BLOCKING**: Non-blocking mode, writes directly to memory via mmap, highest performance
- **ASYNC_BLOCKING**: Async blocking mode, writes to disk in background thread, similar to SharedPreferences.apply()
- **SYNC_BLOCKING**: Sync blocking mode, writes to disk immediately, similar to SharedPreferences.commit()

#### 3. Rich Data Type Support
- **Primitive Types**: boolean, int, float, long, double, String
- **Byte Arrays**: byte[], supports binary data storage
- **String Sets**: Set<String>, fully compatible with SharedPreferences
- **Custom Objects**: Supports arbitrary object serialization through FastEncoder interface
- **Large Value Support**: Automatically handles large data over 64KB using 4-byte length encoding

#### 4. Data Security Assurance
- **Dual File Backup**: A/B files serve as mutual backups, ensuring no data loss
- **Checksum Protection**: Verifies data integrity on every read/write
- **Atomic Operations**: Write operations are atomic, preventing data corruption
- **Auto Degradation**: Automatically switches to blocking mode when mmap fails

#### 5. Data Encryption Support
- **Pluggable Encryption**: Supports injecting custom encryption implementations
- **Transparent Encryption/Decryption**: Encryption occurs before data write, decryption during data parsing
- **Encryption Migration**: Supports smooth migration from plaintext to encrypted data
- **Performance Optimization**: Caches decrypted data, doesn't affect read performance

#### 6. Developer Friendly
- **Good Compatibility**: Implements SharedPreferences interface for easy migration
- **Migration Tool**: Provides adapt() method for automatic SharedPreferences data migration
- **Rich APIs**: Supports batch operations, transaction control, listeners, etc.
- **Type Safety**: Compile-time type checking prevents runtime type errors

#### 7. Stable and Reliable
- **Fault Tolerance**: Basic error detection and automatic recovery
- **Forward Compatibility**: New versions can read old version data
- **Exception Handling**: Basic exception handling and logging

#### 8. Lightweight and Efficient
- **Small Size**: Pure Java implementation, only tens of KB after compilation
- **Memory Friendly**: Reduces unnecessary object creation
- **Fast Startup**: Asynchronous loading

## 2. Usage

### 2.1 Import

```gradle
dependencies {
    implementation 'io.github.billywei01:fastkv:2.6.0'
}
```

### 2.2 Initialization
```kotlin
// Optional: Set global configuration
FastKVConfig.setLogger(FastKVLogger)
FastKVConfig.setExecutor(Dispatchers.IO.asExecutor())
```

Initialization allows optional configuration of logger and executor:
- If no executor is provided, FastKV will create its own CachedThreadPool
- If you provide your own executor, ensure it has concurrent scheduling capability

### 2.3 Basic Usage

```java
// Using Context (recommended)
FastKV kv = new FastKV.Builder(context, "user_data").build();

// Or using custom path
FastKV kv = new FastKV.Builder(path, "user_data").build();

// Basic read/write operations
if (!kv.getBoolean("first_launch")) {
    kv.putBoolean("first_launch", true);
}

int count = kv.getInt("launch_count");
kv.putInt("launch_count", count + 1);

// Supports method chaining
kv.putString("user_name", "John")
  .putInt("user_age", 25)
  .putFloat("user_score", 89.5f);
```

### 2.4 Advanced Configuration

```java
FastKV kv = new FastKV.Builder(context, "secure_data")
    .encoder(new FastEncoder[]{CustomObjectEncoder.INSTANCE})  // Custom encoders
    .cipher(new AESCipher())                                   // Data encryption
    .blocking()                                                // Sync blocking mode
    .build();
```

### 2.5 Store Custom Objects

```java
// 1. Implement FastEncoder interface
public class UserEncoder implements FastEncoder<User> {
    @Override
    public String tag() {
        return "User";
    }
    
    @Override
    public byte[] encode(User user) {
        // Serialization logic
        return userToBytes(user);
    }
    
    @Override
    public User decode(byte[] bytes, int offset, int length) {
        // Deserialization logic
        return bytesToUser(bytes, offset, length);
    }
}

// 2. Register encoder and use
FastEncoder<?>[] encoders = {new UserEncoder()};
FastKV kv = new FastKV.Builder(context, "user_data")
    .encoder(encoders)
    .build();

// 3. Store and retrieve objects
User user = new User("John", 25);
kv.putObject("current_user", user, new UserEncoder());
User savedUser = kv.getObject("current_user");
```

We recommend using the [Packable](https://github.com/BillyWei01/Packable) framework for object serialization.

### 2.6 Data Encryption

```java
// Implement FastCipher interface
public class AESCipher implements FastCipher {
    @Override
    public byte[] encrypt(byte[] src) {
        // Encryption implementation
        return encryptWithAES(src);
    }
    
    @Override
    public byte[] decrypt(byte[] dst) {
        // Decryption implementation
        return decryptWithAES(dst);
    }
    
    // Other encryption methods...
}

// Use encryption
FastKV kv = new FastKV.Builder(context, "secure_data")
    .cipher(new AESCipher())
    .build();
```

### 2.7 Batch Operations

```java
// Batch write
Map<String, Object> data = new HashMap<>();
data.put("name", "John");
data.put("age", 25);
data.put("score", 89.5f);
data.put("active", true);

kv.putAll(data);

// Batch read
Map<String, Object> allData = kv.getAll();

// Transaction control (blocking mode)
kv.disableAutoCommit();
kv.putString("key1", "value1");
kv.putString("key2", "value2");
kv.commit(); // Commit all changes at once
```

### 2.8 Migrate SharedPreferences

```java
public class SpCase {
    public static final String NAME = "common_store";
    
    // Replace original SharedPreferences access
    // public static final SharedPreferences preferences = context.getSharedPreferences(NAME, Context.MODE_PRIVATE);
    
    // Use FastKV with automatic data migration
    public static final SharedPreferences preferences = FastKV.adapt(context, NAME);
}
```

### 2.9 Listen for Data Changes

```java
kv.registerOnSharedPreferenceChangeListener(new SharedPreferences.OnSharedPreferenceChangeListener() {
    @Override
    public void onSharedPreferenceChanged(SharedPreferences sharedPreferences, String key) {
        // Handle data changes
        System.out.println("Key changed: " + key);
    }
});
```

### 2.10 Kotlin Delegate Properties

```kotlin
// Use delegate properties to simplify access
class UserSettings(private val kv: FastKV) {
    var userName: String by kv.string("user_name", "")
    var userAge: Int by kv.int("user_age", 0)
    var isVip: Boolean by kv.boolean("is_vip", false)
}

// Usage example
val settings = UserSettings(kv)
settings.userName = "John"
settings.userAge = 25
println("User: ${settings.userName}, Age: ${settings.userAge}")
```

### 2.11 Important Notes

1. **Path and Name Consistency**: Don't change path and name between versions, or different files will be opened
2. **Cipher Consistency**: If using Cipher, don't change it, or data cannot be parsed (migrating from no encryption to encryption is supported)
3. **Type Consistency**: Value types for the same key should remain consistent
4. **Lifecycle Management**: Call close() method to release resources when no longer needed
5. **Exception Handling**: Properly handle potential exceptions in critical paths

## 3. Performance Benchmark

### Test Environment
- **Test Data**: 600+ real key-value pairs (randomly obfuscated)
- **Test Device**: Huawei P30 Pro
- **Test Method**: Normal distribution input sequence, multiple tests averaged

### Test Results

**Write Performance (milliseconds)**:

| Data Size | 25 | 50 | 100 | 200 | 400 | 600 |
|-----------|----|----|-----|-----|-----|-----|
| SP-commit | 114 | 172 | 411 | 666 | 2556 | 5344 |
| DataStore | 231 | 625 | 1717 | 4421 | 7629 | 13639 |
| SQLiteKV | 192 | 382 | 1025 | 1565 | 4279 | 5034 |
| SP-apply | 3 | 9 | 35 | 118 | 344 | 516 |
| MMKV | 4 | 8 | 5 | 8 | 10 | 9 |
| **FastKV** | **3** | **6** | **4** | **6** | **8** | **10** |

**Read Performance (milliseconds)**:

| Data Size | 25 | 50 | 100 | 200 | 400 | 600 |
|-----------|----|----|-----|-----|-----|-----|
| SP-commit | 1 | 3 | 2 | 1 | 2 | 3 |
| DataStore | 57 | 76 | 115 | 117 | 170 | 216 |
| SQLiteKV | 96 | 161 | 265 | 417 | 767 | 1038 |
| SP-apply | 0 | 1 | 0 | 1 | 3 | 3 |
| MMKV | 0 | 1 | 1 | 5 | 8 | 11 |
| **FastKV** | **0** | **1** | **1** | **3** | **3** | **1** |

### Performance Advantages
- **Write Speed**: Comparable to MMKV, significantly faster than SharedPreferences
- **Read Speed**: Comparable to SharedPreferences, faster than DataStore
- **Startup Speed**: Asynchronous loading, doesn't block app startup
- **Memory Usage**: Compact binary format, less memory consumption

## 4. Architecture Design

FastKV adopts a modular design. For detailed architecture documentation, please refer to [ARCHITECTURE.md](ARCHITECTURE.md).

### Core Modules
- **FastKV**: Core API and business logic
- **FileHelper**: File I/O and backup management
- **DataParser**: Data parsing and serialization
- **GCHelper**: Garbage collection and memory management
- **BufferHelper**: Buffer operations and checksum calculation

### Key Features
- **Dual File Backup**: A/B files ensure data safety
- **Garbage Collection**: Automatic cleanup of invalid data
- **Multiple Writing Modes**: Adapt to different performance requirements
- **Pluggable Encryption**: Support for custom encryption algorithms

## 5. Related Links

- **Technical Blog**: https://juejin.cn/post/7018522454171582500
- **Pure Java Version**: https://github.com/BillyWei01/FastKV-Java
- **Serialization Framework**: https://github.com/BillyWei01/Packable
- **Sample Code**: [FastKV Demo](app/src/main/java/io/fastkv/fastkvdemo/)

## License
See the [LICENSE](LICENSE) file for license rights and limitations.


