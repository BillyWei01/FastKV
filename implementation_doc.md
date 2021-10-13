## 一、编码
文件的布局：

[data_len | checksum | key-value | key-value｜....]

- data_len: 占4字节, 记录所有key-value所占字节数。
- checksum: 占8字节，记录key-value部分的checksum。

key-value的数据布局：

```
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| delete_flag | external_flag | type  | key_len | key_content |  value  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     1bit    |      1bit     | 6bits |  1 byte |             |         |
```

- delete_flag ：标记当前key-value是否删除。
- external_flag: 标记value部分是否写到额外的文件。<br/>
                 注：对于数据量比较大的value,放在主文件会影响其他key-value的访问性能，因此，单独用一个文件来保存该value, 并在主文件中记录其文件名。
- type: value类型，目前支持boolean/int/float/long/double/String/ByteArray以及自定义对象。
- key_len: 记录key的长度，key_len本身占1字节，所有支持key的最大长度为255。
- key_content: key的内容本身，utf8编码。
- value: 基础类型的value, 直接编码（little-end）；<br/>
         其他类型，先记录长度（用varint编码），再记录内容。<br/>
         String采用UTF-8编码，ByteArray无需编码，自定义对象实现Encoder接口，分别在Encoder的encode/decode方法中序列化和反序列化。

## 二、文件存储
- mmap <br/>
  为了提高写入性能，FastKV默认采用mmap的方式写入。
- 降级blocking-I/O <br/>
  当mmap API发生IO异常时，降级到常规的blocking-I/O，同时为了不影响当前线程，会将写入放到异步线程中执行。<br/>
  FastKV也支持用blocking-I/O的方式写文件，只需在获取FastKV实例时指定打开方式即可。
- 数据完整性 <br/>
  如果在写入一部分的过程中发生中断（进程或系统），则文件可能会不完整。<br/>
  故此，需要用一些方法确保数据的完整性。<br/>
  当用mmap的方式打开时，FastKV采用double-write的方式：数据依次写入A/B两个文件，确保任何时刻总有一个文件完整的；<br/>
  加载数据时，通过checksum, 标记，数据合法性检验等方法验证数据的正确性。<br/>
  当用blocking-I/O的写入时，先写临时文件，完整写入后再删除主文件，然后重命名临时文件为主文件。
- 更新策略（增/删/改）<br/>
  新增：写入到数据的尾部。<br/>
  删除：delete_flag设置为1。<br/>
  修改：如果value部分的长度和原来一样，则直接写入原来的位置；<br/>
       否则，先写入key-value到数据尾部，再标记原来位置的delete_flag为1（删除），最后再更新文件的data_len和checksum。
- gc/truncate <br/>
  删除key-value时会收集信息（统计删除的个数，以及所在位置，占用空间等）。<br/>
  GC的触发点有两个：<br/>
  1、新增key-value时剩余空间不足，且一删除的空间达到阈值，且腾出删除空间后足够写入当前key-value, 则触发GC；<br/>
  2、删除key-value时，如果删除空间达到阈值，或者删除的key-value个数达到阈值，则触发GC。<br/>
  GC后如果不用的空间达到设定阈值，则触发truncate（缩小文件大小）。
