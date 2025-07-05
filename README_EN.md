# FastKV 
[![Maven Central](https://img.shields.io/maven-central/v/io.github.billywei01/fastkv)](https://search.maven.org/artifact/io.github.billywei01/fastkv)

FastKV is an efficient key-value storage library written with Java.<br/>

## 1. Features
1. Efficient
    - Binary coding: the size after coding is much smaller than text coding such as XML;
    - Incremental update: FastKV records the offset of each key-value relative to the file head,
      updating can be written directly at the right location.
    - By default, data is recorded with mmap . When updating data, it can be written directly to memory without IO blocking.

2. Support different writing modes
   - In addition to the non-blocking IO (with mmap), 
   FastKV also supports synchronous blocking and asynchronous blocking IO (similar to commit and apply of SharePreferences).

3. Support various types
    - Support primitive types such as Boolean / int / float / long / double / string;
    - Support ByteArray (byte []);
    - Support storage objects.
    - Built in StringSet encoder(for compatibility with SharePreferences).

4. Support data encryption
   - Support for plugin encryption implementations, performing encryption before data is written to disk.
   
5. Support multi-process
   - Focused on single-process scenarios for optimal performance and simplicity.
   - Support listener for changed values, one process write, all processes known.

6. Easy to use
    - FastKV provides rich API interfaces, including getAll() and putAll() methods, it is convenient to migrate the data of frameworks such as SharePreferences to FastKV. 

7. Stable and reliable
    - When FastKV writes data in non-blocking way (mmap), it writes two files one by one,  to ensure that at least one file is integrate at any time;
    - FastKV checks the integrity of the files when loading, if one file is not integrate, it will be restored with another file which is integrated.
    - If mmap API fails, it will be degraded to the blocking I/O; 
     and it will try to restore to mmap mode when reloading.

8. Simple code
    - FastKV is implemented in pure Java and size of jar is just tens of KB.
   
## 2. How to use

### 2.1 Import
FastKV had been published to Maven Central:

```gradle
dependencies {
    implementation 'io.github.billywei01:fastkv:2.6.0'
}
```

### 2.2 Initialization
```kotlin
 FastKVConfig.setLogger(FastKVLogger)
 FastKVConfig.setExecutor(Dispatchers.Default.asExecutor())
```

Initialization is optional.<br/>
You could set log callback and executor.<br/>
It is recommended to set your own thread pool to reuse threads.


### 2.3 Basic cases
- Basic case
```java
 // FastKV kv = new FastKV.Builder(context, name).build();
 FastKV kv = new FastKV.Builder(path, name).build();

 if(!kv.getBoolean("flag")){
     kv.putBoolean("flag" , true);
 }
 
 int count = kv.getInt("count");
 if(count < 10){
     kv.putInt("count" , count + 1);
 }
```

The constructor of Builder can pass Context or path. <br>
If the Context is passed, 
the 'fastkv' directory will be created under the 'files' directory of the internal directory.

### 2.4 Sava custom object
```java
 FastEncoder] encoders = new FastEncoder[]{LongListEncoder.INSTANCE};
 FastKV kv = new FastKV.Builder(path, name).encoder(encoders).build();
     
 List<Long> list = new ArrayList<>();
 list.add(100L);
 list.add(200L);
 list.add(300L);
 kv.putObject("long_list", list, LongListEncoder.INSTANCE);

 List<Long> list2 = kv.getObject("long_list");
```

In addition to supporting basic types, FastKV also supports writing objects. <br/>
You only need to pass in the encoder of the object when building FastKV instances.<br/>
The encoder is an object that implements
[FastEncoder](https://github.com/BillyWei01/FastKV/blob/main/FastKV/src/main/java/io/fastkv/interfaces/FastEncoder.java).<br/>
For example, the code of 'LongListEncoder' like:
[LongListEncoder](https://github.com/BillyWei01/FastKV/blob/main/app/src/androidTest/java/io/fastkv/LongListEncoder.kt)<br>

Encoding objects needs serialization/deserialization. <br/>
Here recommend my serialization project: https://github.com/BillyWei01/Packable

### 2.5 Data encryption
If you need to encrypt data, just pass in the implementation of
[FastCipher](https://github.com/BillyWei01/FastKV/blob/main/fastkv/src/main/java/io/fastkv/interfaces/FastCipher.java)  when creating a FastKV instance

```
FastKV kv = FastKV.Builder(path, name)
         .cipher(yourCihper)
         .build()
```

There are examples of Cipher implementations in the project, 
refer to：[AESCipher](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/fastkv/cipher/AESCipher.java)

### 2.6 Migrate SharePreferences to FastKV

It is easy to migrate SharePreferences to FastKV.

```java
public class SpCase {
   public static final String NAME = "common_store";
   
   // public static final SharedPreferences preferences = AppContext.INSTANCE.getContext().getSharedPreferences(NAME, Context.MODE_PRIVATE);

   public static final SharedPreferences preferences = FastKV.adapt(AppContext.INSTANCE.getContext(), NAME);
}
```

### 2.7 Migrate MMKV to FastKV
Since MMKV does not implement the 'getAll' interface, it cannot be migrated at once like SharePreferences. <br>
But you can create a KV class, create methods such as 'getInt', 'getString', etc., and do adaptation processing in it.<br>
Refer to [MMKV2FastKV](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/storage/MMKV2FastKV.kt)

### 2.8 Multi-Process
FastKV is focused on single-process scenarios for optimal performance and code simplicity.<br>
If your application requires multi-process support, please consider other solutions.<br>

Example:
[MultiProcessTestActivity](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/MultiProcessTestActivity.kt)
and [TestService](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/TestService.kt)

### 2.9 Kotlin delegation
Kotlin is compatible with java, so you could directly use FastKV or SharedPreferences APIs in Kotlin.
In addition, kotlin provides the syntax of delegate, which can be used to improve key-value API.
Refer to [KVData](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/fastkv/kvdelegate/KVData.kt)

## 3. Benchmark
- Data source: Collecting part of the key-value data of SharePreferences in the app (with  confusion) , hundreds of key-values. <br/>
Because some key values are accessed more and others accessed less in normally, <br>
I make a normally distributed sequence to test the accessing.
- Test device: Huawei P30 Pro
- Test code：[Benchmark](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/Benchmark.kt)

Update:

| | 25| 50| 100| 200| 400| 600
---|---|---|---|---|---|---
SP-commit | 114| 172| 411| 666| 2556| 5344
DataStore | 231| 625| 1717| 4421| 7629| 13639
SQLiteKV | 192| 382| 1025| 1565| 4279| 5034
SP-apply | 3| 9| 35| 118| 344| 516
MMKV | 4| 8| 5| 8| 10| 9
FastKV | 3| 6| 4| 6| 8| 10
---

Query:

| | 25| 50| 100| 200| 400| 600
---|---|---|---|---|---|---
SP-commit | 1| 3| 2| 1| 2| 3
DataStore | 57| 76| 115| 117| 170| 216
SQLiteKV | 96| 161| 265| 417| 767| 1038
SP-apply | 0| 1| 0| 1| 3| 3
MMKV | 0| 1| 1| 5| 8| 11
FastKV | 0| 1| 1| 3| 3| 1

# Java-Version
There is a project write with only API of JDK, no Android SDK. <br>
link: https://github.com/BillyWei01/FastKV-Java

## License
See the [LICENSE](LICENSE) file for license rights and limitations.


