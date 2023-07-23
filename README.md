# FastKV
[![Maven Central](https://img.shields.io/maven-central/v/io.github.billywei01/fastkv)](https://search.maven.org/artifact/io.github.billywei01/fastkv)｜[English](README_EN.md)

## 1. 概述
FastKV是用Java编写的高效可靠的key-value存储库。<br>

FastKV有以下特点：
1. 读写速度快
    - 二进制编码，编码后的体积相对XML等文本编码要小很多；
    - 增量编码：FastKV记录了各个key-value相对文件的偏移量，
      从而在更新数据时可以直接在指定的位置写入数据。
    - 默认用mmap的方式记录数据，更新数据时直接写入到内存即可，没有IO阻塞。
    - 对超大字符串和大数组做特殊处理，另起文件写入，不影响主文件的加载和更新。
2. 支持多种写入模式
   - 除了mmap这种非阻塞的写入方式，FastKV也支持常规的阻塞式写入方式，
     并且支持同步阻塞和异步阻塞（分别类似于SharePreferences的commit和apply)。
3. 支持多种类型
   - 支持常用的boolean/int/float/long/double/String等基础类型。
   - 支持ByteArray (byte[])。
   - 支持存储自定义对象。
   - 内置Set<String>的编码器 (兼容SharePreferences)。
4. 支持数据加密
   - 支持注入加密解密的实现，在数据写入磁盘之前执行加密。
   - 解密处理发生在数据解析阶段，解析完成后，数据是缓存的（用HashMap缓存)，<br>
     所以加解密会稍微增加写入和解析的时间，不会增加索引数据的时间。
5. 支持多进程
   - 项目提供了支持多进程的存储类（MPFastKV)。
   - 支持监听文件内容变化，其中一个进程修改文件，所有进程皆可感知。
6. 方便易用
   - FastKV提供了了丰富的API接口，开箱即用。
   - 提供的接口其中包括getAll()和putAll()方法，
     所以很方便迁移SharePreferences等框架的数据到FastKV, 当然，迁移FastKV的数据到其他框架也很简单。
7. 稳定可靠
   - 通过double-write等方法确保数据的完整性。
   - 在API抛IO异常时自动降级处理。
8. 代码精简
   - FastKV由纯Java实现，编译成jar包后体积只有数十K。
   
## 2. 使用方法

### 2.1 导入

```gradle
dependencies {
    implementation 'io.github.billywei01:fastkv:2.0.2'
}
```

### 2.2 初始化
```kotlin
    FastKVConfig.setLogger(FastKVLogger)
    FastKVConfig.setExecutor(Dispatchers.Default.asExecutor())
```
初始化可以按需设置日志接口和Executor。


### 2.3 基本用法

```java
    // FastKV kv = new FastKV.Builder(path, name).build();
    FastKV kv = new FastKV.Builder(context, name).build();
    
    if(!kv.getBoolean("flag")){
        kv.putBoolean("flag" , true);
    }
    
    int count = kv.getInt("count");
    if(count < 10){
        kv.putInt("count" , count + 1);
    }
```

Builder的构造可传Context或者path。<br>
如果传Context的话，会在内部目录的'files'目录下创建'fastkv'目录来作为文件的保存路径。

### 2.4 存储自定义对象

```java
    FastEncoder<?>[] encoders = new FastEncoder[]{LongListEncoder.INSTANCE};
    FastKV kv = new FastKV.Builder(context, name).encoder(encoders).build();
        
    List<Long> list = new ArrayList<>();
    list.add(100L);
    list.add(200L);
    list.add(300L);
    kv.putObject("long_list", list, LongListEncoder.INSTANCE);
    
    List<Long> list2 = kv.getObject("long_list");
```

除了支持基本类型外，FastKV还支持写入对象，只需在构建FastKV实例时传入对象的编码器即可。<br>
编码器为实现FastEncoder接口的对象。<br>
上面LongListEncoder就实现了FastEncoder接口，代码实现可参考：
[LongListEncoder](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/fastkv/LongListEncoder.kt)<br>

编码对象涉及序列化/反序列化。<br>
这里推荐笔者的另外一个框架：https://github.com/BillyWei01/Packable

### 2.5 数据加密
如需对数据进行加密，在创建FastKV实例时传入
[FastCipher](https://github.com/BillyWei01/FastKV/blob/main/FastKV/src/main/java/io/fastkv/interfaces/FastCipher.java) 的实现即可。

```
FastKV kv = FastKV.Builder(path, name)
         .cipher(yourCihper)
         .build()
```

项目中有举例Cipher的实现，可参考：[AESCipher](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/fastkv/AESCipher.java)

### 2.6 迁移 SharePreferences 到 FastKV

SP支持getAll接口，而FastKV支持putAll接口，所以导入SP数据到FastKV很简单。

```java
public class CommonStore {
    public static final String NAME = "common_store";
    // 原本的获取SP的方法
    // public static final SharedPreferences preferences = GlobalConfig.appContext.getSharedPreferences(NAME, Context.MODE_PRIVATE);

    // 导入原SP数据
    public static final SharedPreferences preferences = FastPreferences.adapt(GlobalConfig.appContext, NAME);
}
```

FastPreferences的代码实现：[FastPreferences](https://github.com/BillyWei01/FastKV/blob/main/FastKV/src/main/java/io/fastkv/FastPreferences.java) <br>
FastPreferences是SharedPreferences的实现类，由于接口不变，用FastPreferences替换之后，不需要改动其他代码。<br>

### 2.7 迁移 MMKV 到 FastKV
由于MMKV没有实现 'getAll' 接口，所以无法像SharePreferences一样一次性迁移。<br>
但是可以封装一个KV类，创建 'getInt'，'getString' ... 等方法，并在其中做适配处理。
可参考：[FooKV](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/storage/FooKV.kt)

### 2.8 多进程
项目提供了支持多进程的实现：[MPFastKV](https://github.com/BillyWei01/FastKV/blob/main/FastKV/src/main/java/io/fastkv/MPFastKV.java)。<br>
MPFastKV除了支持多进程读写之外，还实现了SharedPreferences的接口，包括支持注册OnSharedPreferenceChangeListener;<br>
其中一个进程修改了数据，所有的进程都会感知（通过OnSharedPreferenceChangeListener回调）。<br>
可参考 [MultiProcessTestActivity](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/MultiProcessTestActivity.kt) 
和 [TestService](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/TestService.kt)

需要提醒的是，由于支持多进程需要维护更多的状态，MPFastKV 的写入要比FastKV慢不少，
所以在不需要多进程访问的情况下，尽量用 FastKV 或 FastPreferences。

### 2.9 Kotlin 委托
Kotlin是兼容Java的，所以Kotlin下也可以直接用FastKV或者SharedPreferences的API。 <br>
此外，Kotlin还提供了“委托属性”这一语法糖，可以用于改进key-value API访问。 <br>
可参考：[KVData](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/fastkv/KVData.kt)

## 3. 性能测试
- 测试数据：搜集APP中的SharePreferences汇总的部份key-value数据（经过随机混淆）得到总共四百多个key-value。<br>
          由于日常使用过程中部分key-value访问多，部分访问少，所以构造了一个正态分布的访问序列。<br>
          此过程循环多遍，分别求其总耗时。
- 比较对象： SharePreferences/DataStore/MMKV。
- 测试机型：荣耀20S。

测试代码：[Benchmark](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/Benchmark.kt)

测试结果：

| | 写入(ms) |读取(ms) 
---|---|---
SharePreferences | 1182 | 2
DataStore | 33277 | 2
MMKV | 29 | 10
FastKV  | 19 | 1 

- SharePreferences提交用的是apply, 耗时依然不少。
- DataStore写入很慢。
- MMKV的读取比SharePreferences/DataStore要慢，写入则比之快不少。
- FastKV无论读取还是写入都比其他方式要快。

现实中通常情况下不会有此差距，因为一般而言不会几百个key-value写到同一个文件，此处仅为显示极端情况下的对比，读者可自行调整参数看对比数据。

## 4. 参考链接
相关博客： <br>
https://juejin.cn/post/7018522454171582500

由于提供给Android平台的版本和纯JDK的版本的差异越来越多，所以分开仓库来维护。<br>
纯JDK版本的链接为：<br>
https://github.com/BillyWei01/FastKV-Java

## 5. 讨论
创建了一个QQ群：436359235 <br>
对FastKV感兴趣的朋友可以加群讨论，提建议，提BUG，都可以。

## License
See the [LICENSE](LICENSE) file for license rights and limitations.



