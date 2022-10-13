# FastKV 
[![Maven Central](https://img.shields.io/maven-central/v/io.github.billywei01/fastkv)](https://search.maven.org/artifact/io.github.billywei01/fastkv)｜[中文文档](README_CN.md)


FastKV is an efficient key-value storage library written with Java.<br/>
It can be used on  platforms with JVM environment, such as Android.

## 1. Features
1. Efficient
    - Binary coding: the size after coding is much smaller than text coding such as XML;
    - Incremental update: FastKV records the offset of each key-value relative to the file head,
      updating can be written directly at the right location.
    - By default, data is recorded with mmap . When updating data, it can be written directly to memory without IO blocking.
    - For a value which length is larger than the threshold, it will be written to another file separately,  
      only it's file name will be cached. In that way, it will not slow down accessing of other key-value .

2. Support different writing modes
   - In addition to the non-blocking IO (with mmap), 
   FastKV also supports synchronous blocking and asynchronous blocking IO (similar to commit and apply of SharePreferences).

3. Support various types
    - Support primitive types such as Boolean / int / float / long / double / string;
    - Support ByteArray (byte []);
    - Support storage objects.
    - Built in StringSet encoder(for compatibility with SharePreferences).

4. Support multi-process
   - The project supply an implement to support multi-process (MPFastKV).
   - Support listener for changed values, one process write, all processes known.

5. Easy to use
    - FastKV provides rich API interfaces, including getAll() and putAll() methods, it is convenient to migrate the data of frameworks such as SharePreferences to FastKV. 

6. Stable and reliable
    - When FastKV writes data in non-blocking way (mmap), it writes two files one by one,  to ensure that at least one file is integrate at any time;
    - FastKV checks the integrity of the files when loading, if one file is not integrate, it will be restored with another file which is integrated.
    - If mmap API fails, it will be degraded to the blocking I/O; 
     and it will try to restore to mmap mode when reloading.

7. Simple code
    - FastKV is implemented in pure Java and size of jar is less then 100KB.
   
## 2. Getting Start

### 2.1 Import
FastKV had been published to Maven Central:

For Android (include SharePreferences API, support multi-process):

```gradle
dependencies {
    implementation 'io.github.billywei01:fastkv:1.1.3'
}
```

For Java (Pure Java API, not support multi-process):

```gradle
dependencies {
    implementation 'io.github.billywei01:fastkv-java:1.1.3'
}
```


### 2.2 Initialization
```kotlin
    FastKVConfig.setLogger(FastKVLogger)
    FastKVConfig.setExecutor(ChannelExecutorService(4))
```

Initialization is optional.<br/>
You could set log callback and executor.<br/>
It is recommended to set your own thread pool to reuse threads.

The log interface provides three levels of callbacks.
```java
    public interface Logger {
        void i(String name, String message);

        void w(String name, Exception e);

        void e(String name, Exception e);
    }

```

### 2.3 Read/Write
- Basic case
```java
    FastKV kv = new FastKV.Builder(path, name).build();
   
    if(!kv.getBoolean("flag")){
        kv.putBoolean("flag" , true);
    }
    
    int count = kv.getInt("count");
    if(count < 10){
        kv.putInt("count" , count + 1);
    }
```

- Sava custom object
```java
    FastKV.Encoder<?>[] encoders = new FastKV.Encoder[]{LongListEncoder.INSTANCE};
    FastKV kv = new FastKV.Builder(path, name).encoder(encoders).build();
        
    List<Long> list = new ArrayList<>();
    list.add(100L);
    list.add(200L);
    list.add(300L);
    kv.putObject("long_list", list, LongListEncoder.INSTANCE);
   
    List<Long> list2 = kv.getObject("long_list");
```


In addition to supporting basic types, FastKV also supports writing objects. You only need to pass in the encoder of the object when building FastKV instances.<br/>
The encoder is an object that implements FastKV.Encoder.<br/>
For example, the implementation of LongListEncoder like this:

```java
public class LongListEncoder implements FastKV.Encoder<List<Long>> {
    public static final LongListEncoder INSTANCE = new LongListEncoder();

    @Override
    public String tag() {
        return "LongList";
    }

    @Override
    public byte[] encode(List<Long> obj) {
        return new PackEncoder().putLongList(0, obj).getBytes();
    }

    @Override
    public List<Long> decode(byte[] bytes, int offset, int length) {
        return PackDecoder.newInstance(bytes, offset, length).getLongList(0); 
    }
}
```

Encoding objects needs serialization/deserialization. <br/>
Here recommend my serialization project: https://github.com/BillyWei01/Packable

### 2.4 For Android 
Comparing with common usage, Android platform has SharePreferences API and support Kotlin.<br/>
See: [Android Case](android_case.md)


## 3. Benchmark
- Data source: Collecting part of the key-value data of SharePreferences in the app (with  confusion) , hundreds of key-values. <br/>
Because some key values are accessed more and others accessed less in normally, 
I make a normally distributed sequence to test the accessing.

- Test Code：[Benchmark](https://github.com/BillyWei01/FastKV/blob/main/FastKVDemo/app/src/main/java/io/fastkv/fastkvdemo/Benchmark.kt)
- Comparison component: SharePreferences/DataStore/MMKV
- Device: Huawei Honor 20s

Result:

| | Write(ms) | Read(ms) 
---|---|---
SharePreferences | 1182 | 2
DataStore | 33277 | 2
MMKV | 29 | 10
FastKV  | 19 | 1 

- SharePreferences use the apply mode. When use commit mode, it will be much slower.
- DataStore writes data very slow.
- MMKV read slower than SharePreferences/DataStore，but much faster in writing.
- FastKV is the fastest both in writing or reading.

The test above writes hundreds of key-values on one file, so the results have a big difference.
Normally one file may only save several or tens of key-values, the result may be close.

## License
See the [LICENSE](LICENSE) file for license rights and limitations.


