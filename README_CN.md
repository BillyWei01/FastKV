# FastKV

## 1. 概述
FastKV是用Java编写的高效可靠的key-value存储组件。<br>
可以用于各种有JVM环境的运行平台，比如Android。

FastKV有以下特点：
1. 读写速度快
    - 二进制编码，编码后的体积相对XML等文本编码要小很多；
    - 增量编码：FastKV记录了各个key-value相对文件的偏移量（包括失效的key-value），
      从而在更新数据时可以直接在指定的位置写入数据。
    - 默认用mmap的方式记录数据，更新数据时直接写入到内存即可，没有IO阻塞。
2. 支持多种写入模式
   - 除了mmap这种非阻塞的写入方式，FastKV也支持常规的阻塞式写入方式，
     并且支持同步阻塞和异步阻塞（分别类似于SharePreferences的commit和apply)。
3. 支持多种类型
   - 支持常用的boolean/int/float/long/double/String等基础类型；
   - 支持ByteArray (byte[])；
   - 支持存储自定义对象。
   - 内置Set<String>的编码器。(为了方便兼容SharePreferences)。
4. 方便易用
   - FastKV提供了了丰富的API接口，开箱即用。
   - 提供的接口其中包括getAll()和putAll()方法，
     所以很方便迁移SharePreferences等框架的数据到FastKV, 当然，迁移FastKV的数据到其他框架也很方便。
5. 稳定可靠
   - 通过double-write等方法确保数据的完整性。
   - 在API抛IO异常时提供降级处理。
6. 代码精简
   - FastKV由纯Java实现，编译成jar包后体积仅30多K。
   
## 2. 使用方法

### 2.1 导入
FastKV 已发布到Maven中央仓库，路径如下:
```gradle
dependencies {
    implementation 'io.github.billywei01:fastkv:1.0.2'
}
```

### 2.2 初始化
```kotlin
    FastKVConfig.setLogger(FastKVLogger)
    FastKVConfig.setExecutor(ChannelExecutorService(4))
```
初始化可以按需设置日志回调和Executor。
建议传入自己的线程池，以复用线程。

日志接口提供三个级别的回调，按需实现即可。
```java
    public interface Logger {
        void i(String name, String message);

        void w(String name, Exception e);

        void e(String name, Exception e);
    }

```

### 2.3 数据读写
- 基本用法
```java
    FastKV kv = new FastKV.Builder(path, name).build();
    if(!kv.getBoolean("flag")){
        kv.putBoolean("flag" , true);
    }
```


- 存储自定义对象

```java
    FastKV.Encoder<?>[] encoders = new FastKV.Encoder[]{LongListEncoder.INSTANCE};
    FastKV kv = new FastKV.Builder(path, name).encoder(encoders).build();
        
    String objectKey = "long_list";
    List<Long> list = new ArrayList<>();
    list.add(100L);
    list.add(200L);
    list.add(300L);
    kv.putObject(objectKey, list, LongListEncoder.INSTANCE);

    List<Long> list2 = kv.getObject("long_list");
```

除了支持基本类型外，FastKV还会支持写入对象，只需在构建FastKV实例时传入对象的编码器即可。
编码器为实现FastKV.Encoder的对象。
比如上面的LongListEncoder的实现如下：

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
        PackDecoder decoder = PackDecoder.newInstance(bytes, offset, length);
        List<Long> list = decoder.getLongList(0);
        decoder.recycle();
        return (list != null) ? list : new ArrayList<>();
    }
}
```

编码对象涉及序列化/反序列化。<br/>
这里推荐笔者的另外一个框架：https://github.com/BillyWei01/Packable

- blocking I/O <br/>
FastKV默认采用mmap的方式写入，mmap默认情况下由系统定时刷入磁盘，若在刷盘前断电或系统崩溃，会丢失更新（原来的数据不会丢失）。
对于非常重要的数据，需确保更新真正了写入磁盘，一种方式是调用force()强制刷盘，另一种方式是使用常规的blocking I/O写入。
要使用blocking I/O，在构造FastKV时调用blocking()或者asyncBlocking()函数即可。<br/>
用法如下：

```java
    FastKV kv = new FastKV.Builder(TestHelper.DIR, "test").blocking().build();
    // auto commit
    kv.putLong("time", System.currentTimeMillis());

    // custom commit
    kv.disableAutoCommit();
    kv.putLong("time", System.currentTimeMillis());
    kv.putString("str", "hello");
    kv.putInt("int", 100);
    boolean success = kv.commit();
    if (success) {
        // handle success
    } else {
        // handle failed
    }
```

### 2.4 用法 For Android
相对于常规用法，Android平台主要是多了SharePreferences API, 以及支持Kotlin。<br/>
具体参考：[用法 For Android](android_case_CN.md)

## 3. 性能测试
- 测试数据：搜集APP中的SharePreferenses汇总的部份key-value数据（经过随机混淆）得到总共四百多个key-value。<br>
          由于日常使用过程中部分key-value访问多，部分访问少，所以构造了一个正态分布的访问序列。
- 比较对象： SharePreferences 和 MMKV
- 测试机型：荣耀20S

分别读写10次，耗时如下：

| | 写入(ms) |读取(ms) 
---|---|---
SharePreferences | 1490 | 6 
MMKV | 34 | 9 
FastKV  | 14 | 1 

- SharePreferences提交用的是apply, 耗时依然不少，用commit在该机器上需要五千多毫秒。
- MMKV的读取比SharePreferences要慢一些，写入则比之快许多；
- FastKV无论读取还是写入都比另外两种方式要快。

## 原理
参考：[实现要点](implementation_doc.md)

## License
See the [LICENSE](LICENSE) file for license rights and limitations.



