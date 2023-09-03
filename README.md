# FastKV
[![Maven Central](https://img.shields.io/maven-central/v/io.github.billywei01/fastkv)](https://search.maven.org/artifact/io.github.billywei01/fastkv) | [English](README_EN.md)

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
     所以加解密会稍微增加写入(put)和解析(loading)的时间，不会增加索引数据(get)的时间。
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
    implementation 'io.github.billywei01:fastkv:2.2.7'
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

项目中有举例Cipher的实现，可参考：[AESCipher](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/fastkv/cipher/AESCipher.java)

### 2.6 迁移 SharePreferences 到 FastKV

FastKV实现了SharedPreferences接口，并且提供了迁移SP数据的方法。<br>
用法如下：

```java
public class SpCase {
   public static final String NAME = "common_store";
   // 原本的获取SP的方法
   // public static final SharedPreferences preferences = GlobalConfig.appContext.getSharedPreferences(NAME, Context.MODE_PRIVATE);
   
   // 导入原SP数据
   public static final SharedPreferences preferences = FastKV.adapt(AppContext.INSTANCE.getContext(), NAME);
}
```

### 2.7 迁移 MMKV 到 FastKV
由于MMKV没有实现 'getAll' 接口，所以无法像SharePreferences一样一次性迁移。<br>
但是可以封装一个KV类，创建 'getInt'，'getString' ... 等方法，并在其中做适配处理。
可参考：[FooKV](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/data/FooKV.kt)

### 2.8 多进程
项目提供了支持多进程的实现：[MPFastKV](https://github.com/BillyWei01/FastKV/blob/main/FastKV/src/main/java/io/fastkv/MPFastKV.java)。<br>
MPFastKV除了支持多进程读写之外，还实现了SharedPreferences的接口，包括支持注册OnSharedPreferenceChangeListener;<br>
其中一个进程修改了数据，所有的进程都会感知（通过OnSharedPreferenceChangeListener回调）。<br>
可参考 [MultiProcessTestActivity](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/MultiProcessTestActivity.kt) 
和 [TestService](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/TestService.kt)

需要提醒的是，由于支持多进程需要维护更多的状态，MPFastKV 的写入要比FastKV慢不少，
所以在不需要多进程访问的情况下，尽量用 FastKV。

### 2.9 Kotlin 委托
Kotlin是兼容Java的，所以Kotlin下也可以直接用FastKV或者SharedPreferences的API。 <br>
此外，Kotlin还提供了“委托属性”这一语法糖，可以用于改进key-value API访问。 <br>
可参考：[KVData](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/fastkv/KVData.kt)

### 2.10 注意事项
1. 不同版本之间，不要改变路径和名字，否则会打开不同的文件。 <br>
2. 如果使用了Cipher(加密)，不要更换，否则会打开文件时会解析不了。
   不过从没有使用Cipher到使用Cipher是可以的，FastKV会先解析未加密的数据，然后在重新加密写入<br>
3. 同一个key, 对应的value需要从一而终。
   例如，上一个版本是putString，下一个版本切换到putInt，会抛出类型转换异常。 <br>
   再比如，同一个key, A处putString, B处putInt (不管是否同一个版本)，也会抛出类型转换异常。

## 3. 性能测试
- 测试数据：搜集APP中的SharePreferences汇总的部份key-value数据（经过随机混淆）得到总共六百多个key-value。<br>
  分别截取其中一部分，构造正态分布的输入序列，进行多次测试。
- 测试机型：华为P30 Pro
- 测试代码：[Benchmark](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/Benchmark.kt)

测试结果如下，第一行为kv数量，第一列为读写方式。

写入：

|	| 25	| 50	| 100 |	200|	400	| 600
---|---|---|---|---|---|---
SP-commit	|121|	189	|443	|773	|2113	|4801
DataStore	|94	|226	|524	|1332	|4422	|10066
SQLiteKV	|161	|333	|663	|1353	|3159	|4299
FastKV-commit	|107	|150	|338	|601	|1359	|2235
SP-apply	|2	|10	|37	|109	|279	|519
MMKV	|3	|3	|6	|6	|11	|15
FastKV-mmap	|1	|2	|3	|5	|14	|11


----

读取：

|	|25	|50	|100	|200	|400	|600
---|---|---|---|---|---|---
SP-commit	|0	|1	|2	|1	|1	|1
DataStore	|25	|5	|2	|1	|1	|2
SQLiteKV	|112	|183	|281	|480	|740	|1051
FastKV-commit	|0	|1	|1	|2	|3	|2
SP-apply	|0	|1	|1	|1	|2	|3
MMKV		|0	|1	|3	|3	|8	|11
FastKV-mmap	|0	|1	|1	|2	|1	|1

关于谁快谁慢就不一一描述了，尽在上面的表格中。
这里补充一些其他的内容：

- [SQLiteKV](https://github.com/BillyWei01/FastKV/blob/main/app/src/main/java/io/fastkv/fastkvdemo/sqlitekv/SQLiteKV.java) 目前没加内存缓存，所以读取比较慢，加个HashMap就和其他的差不多了，但是要加内存缓存的话需要加一些编码来标记存入时的类型。
- SQLite虽然也是”增量写入“，但是相对其他“轻量级”存储而言，执行路径确实比较长，所以总体也是比较耗时。
- 前4种写入方式是同步写入，在put方法返回的时候，数据是写入到磁盘了。
- SP-apply为异步写入，put函数结束时提交了一份数据到队列，并不意味着数据就写入磁盘了。
- 后两种通过mmap写入，put函数结束后数据就写到内核空间了，除非系统崩溃或者断电，否则数据会在适当时机由系统写入。
- MMKV在执行写入数据到mmap内存时，如果程序中断，可能会导致文件损坏。
- FastKV-commit比SP-commit， 快一些，但不多，应该就是快在序列化（FastKV的序列化是增量的）。<br>
  FastKV-commit也会在每一次put操作时调用`sync`将数据写盘。<br>
  我尝试过不调用`sync`，速度能快几倍，所以文章前面我说序列化不是SP耗时的主要原因。<br>
  因为已经提供了高吞吐量的FastKV-mmap模式（默认）了，所以在FastKV-commit的模式中，我选择调用`sync`， 正如前面分析的，“以牺牲系统 I/O 吞吐量作为代价，确保数据落盘”。<br>
- 当然，FastKV-mmap模式也是基本可靠的：<br>
   - 一旦put操作完成，除非系统崩溃或者设备异常等发生，否则数据最终会落盘，即使进程退出；
   - put操作过程中进程退出，也不会导致数据不完整（这一点前面有分析）。
   - 不过在put操作完成后，数据落盘之前，如果发生系统崩溃的情况，put操作所包含的更新就写不到磁盘了。<br>
     SP-apply也存在类似的情况，而且其发生的概率更高（并不需要系统崩溃，进程退出就可能发生）。<br>
- 异步写入，或者非阻塞写入，都可能发生“put操作借结束，但update不生效（在生效之前发生异常）“。<br>
  所以，如果对数据一致性有较高要求，建议用“同步写入”。

## 4. 参考链接
相关博客： <br>
https://juejin.cn/post/7018522454171582500

由于提供给Android平台的版本和纯JDK的版本的差异越来越多，所以分开仓库来维护。<br>
纯JDK版本的链接为：<br>
https://github.com/BillyWei01/FastKV-Java

## License
See the [LICENSE](LICENSE) file for license rights and limitations.



