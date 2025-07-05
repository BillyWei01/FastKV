package io.fastkv;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Handler;
import android.os.Looper;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import io.fastkv.interfaces.FastCipher;
import io.fastkv.interfaces.FastEncoder;
import io.fastkv.interfaces.FastLogger;

import io.fastkv.Container.*;

/**
 * FastKV - 高性能键值存储库
 * 
 * <h2>架构组成（静态结构）</h2>
 * FastKV采用模块化设计，主要由以下组件构成：
 * <ul>
 * <li><b>FastKV核心类</b>：主要负责API接口、数据管理和业务逻辑协调</li>
 * <li><b>FileHelper</b>：文件I/O操作模块，处理A/B/C文件的读写、重写、备份恢复等</li>
 * <li><b>DataParser</b>：数据解析模块，负责二进制数据的编码解码和容器创建</li>
 * <li><b>GCHelper</b>：垃圾回收模块，处理内存整理、缓冲区扩容和无效数据清理</li>
 * <li><b>BufferHelper</b>：缓冲区工具模块，提供数据打包、校验和计算等底层操作</li>
 * <li><b>LoggerHelper</b>：日志管理模块，统一处理各模块的日志输出</li>
 * <li><b>Container系列</b>：数据容器，包装不同类型的值并记录元数据</li>
 * </ul>
 * 
 * <h2>文件存储结构</h2>
 * <ul>
 * <li><b>.kva/.kvb文件</b>：双备份主数据文件，使用mmap内存映射技术</li>
 * <li><b>.kvc文件</b>：阻塞模式下的完整数据文件</li>
 * <li><b>.tmp文件</b>：临时文件，用于原子性写入操作</li>
 * </ul>
 * 
 * <h2>运作模式（动态行为）</h2>
 * 
 * <h3>三种写入模式</h3>
 * <ul>
 * <li><b>NON_BLOCKING</b>：通过mmap写入部分数据，高性能但可能在系统崩溃时丢失最新更新</li>
 * <li><b>ASYNC_BLOCKING</b>：异步使用阻塞I/O将所有数据写入磁盘，类似SharedPreferences的apply()</li>
 * <li><b>SYNC_BLOCKING</b>：同步写入所有数据，类似SharedPreferences的commit()，最安全但性能较低</li>
 * </ul>
 * 
 * <h3>数据加载流程</h3>
 * <ol>
 * <li>优先尝试从C文件加载（阻塞模式数据）</li>
 * <li>如果C文件不存在且为非阻塞模式，则从A/B文件加载</li>
 * <li>验证数据完整性（校验和验证）</li>
 * <li>解析二进制数据并创建对应的Container对象</li>
 * <li>如需要加密重写，则执行数据迁移</li>
 * </ol>
 * 
 * <h3>数据写入流程</h3>
 * <ol>
 * <li>数据编码：将值序列化为二进制格式</li>
 * <li>缓冲区管理：检查空间，必要时进行GC或扩容</li>
 * <li>内存更新：更新FastBuffer和Container对象</li>
 * <li>持久化：根据写入模式同步到文件</li>
 * <li>校验和更新：维护数据完整性</li>
 * </ol>
 * 
 * <h3>垃圾回收机制</h3>
 * <ul>
 * <li>跟踪无效数据段，当无效数据超过阈值时触发GC</li>
 * <li>整理内存布局，移除无效数据，压缩存储空间</li>
 * <li>支持缓冲区扩容和收缩以适应数据量变化</li>
 * </ul>
 * 
 * <h3>容错和恢复</h3>
 * <ul>
 * <li>双文件备份机制（A/B文件）确保数据安全</li>
 * <li>校验和验证检测数据损坏</li>
 * <li>自动降级：mmap失败时转为阻塞模式</li>
 * <li>向前兼容：支持读取旧版本外部文件并自动迁移</li>
 * </ul>
 * 
 * <h2>使用注意事项</h2>
 * <ul>
 * <li>一旦创建后不要更改文件名</li>
 * <li>一旦创建后不要更改加密器（但从无加密状态应用加密器是可以的）</li>
 * <li>不要为同一个key更改值类型</li>
 * <li>当前版本已移除大值外部文件的写入功能，但保留读取逻辑以确保向前兼容</li>
 * </ul>
 */
@SuppressWarnings("rawtypes")
public final class FastKV implements SharedPreferences, SharedPreferences.Editor {

    // ==================== 常量定义 ====================
    
    private static final String ENCRYPT_FAILED = "Encrypt failed";

    static final String A_SUFFIX = ".kva";
    static final String B_SUFFIX = ".kvb";
    static final String C_SUFFIX = ".kvc";
    static final String TEMP_SUFFIX = ".tmp";

    static final int DATA_SIZE_LIMIT = 1 << 28; // 256M
    static final int CIPHER_MASK = 1 << 30;

    private static final byte[] EMPTY_ARRAY = new byte[0];
    static final int[] TYPE_SIZE = {0, 1, 4, 4, 8, 8};
    static final int DATA_START = 12;

    static final int PAGE_SIZE = Utils.getPageSize();

    // ==================== 实例字段 ====================
    
    final String path;
    final String name;
    final Map<String, FastEncoder> encoderMap;
    final FastLogger logger = FastKVConfig.sLogger;
    final FastCipher cipher;

    int dataEnd;
    long checksum;
    final HashMap<String, BaseContainer> data = new HashMap<>();
    volatile boolean startLoading = false;

    FastBuffer fastBuffer;
    int updateStart;
    int updateSize;

    final List<String> deletedFiles = new ArrayList<>();

    // 如果之前没有加密，而这次打开需要加密，则需要重写数据。
    boolean needRewrite = false;

    boolean closed = false;

    final Executor applyExecutor = new LimitExecutor();

    int invalidBytes;
    final ArrayList<Segment> invalids = new ArrayList<>();

    final ArrayList<SharedPreferences.OnSharedPreferenceChangeListener> listeners = new ArrayList<>();
    final Handler mainHandler = new Handler(Looper.getMainLooper());

    // 原始 FastKV 字段
    FileChannel aChannel;
    FileChannel bChannel;
    MappedByteBuffer aBuffer;
    MappedByteBuffer bBuffer;

    int removeStart;

    // 默认写入模式是非阻塞的（通过 mmap 写入部分数据）。
    // 如果 mmap API 抛出 IOException，则降级为阻塞模式（使用阻塞 I/O 将所有数据写入磁盘）。
    // 用户可以通过 FastKV.Builder 指定使用阻塞模式
    static final int NON_BLOCKING = 0;
    static final int ASYNC_BLOCKING = 1;
    static final int SYNC_BLOCKING = 2;
    int writingMode;

    // 仅在模式不是 NON_BLOCKING 时生效
    boolean autoCommit = true;

    // ==================== 构造函数和初始化 ====================

    FastKV(final String path,
           final String name,
           FastEncoder[] encoders,
           FastCipher cipher,
           int writingMode) {
        this.path = path;
        this.name = name;
        this.cipher = cipher;
        this.writingMode = writingMode;
        
        Map<String, FastEncoder> map = new HashMap<>();
        if (encoders != null) {
            for (FastEncoder e : encoders) {
                String tag = e.tag();
                if (map.containsKey(tag)) {
                    LoggerHelper.error(this, "duplicate encoder tag:" + tag);
                } else {
                    map.put(tag, e);
                }
            }
        }
        StringSetEncoder encoder = StringSetEncoder.INSTANCE;
        map.put(encoder.tag(), encoder);
        this.encoderMap = map;

        synchronized (data) {
            FastKVConfig.getExecutor().execute(this::loadData);
            while (!startLoading) {
                try {
                    // 等待直到 loadData() 获得对象锁
                    data.wait();
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    private synchronized void loadData() {
        // 一旦获得对象锁，通知等待者继续构造函数
        synchronized (data) {
            startLoading = true;
            data.notify();
        }
        long start = System.nanoTime();
        if (!FileHelper.loadFromCFile(this) && writingMode == NON_BLOCKING) {
            FileHelper.loadFromABFile(this);
        }
        if (fastBuffer == null) {
            fastBuffer = new FastBuffer(PAGE_SIZE);
        }
        if (dataEnd == 0) {
            dataEnd = DATA_START;
        }
        if (needRewrite) {
            FileHelper.rewrite(this);
            LoggerHelper.info(this, "rewrite data");
        }
        if (logger != null) {
            long t = (System.nanoTime() - start) / 1000000;
            LoggerHelper.info(this, "loading finish, data len:" + dataEnd + ", get keys:" + data.size() + ", use time:" + t + " ms");
        }
    }

    private int packSize(int size) {
        return BufferHelper.packSize(size, cipher != null);
    }


    // SharedPreferences 接口方法
    public synchronized boolean contains(String key) {
        return data.containsKey(key);
    }

    public synchronized boolean getBoolean(String key) {
        return getBoolean(key, false);
    }

    public synchronized boolean getBoolean(String key, boolean defValue) {
        BaseContainer c = data.get(key);
        return c == null ? defValue : c.toBoolean();
    }

    public int getInt(String key) {
        return getInt(key, 0);
    }

    public synchronized int getInt(String key, int defValue) {
        BaseContainer c = data.get(key);
        return c == null ? defValue : c.toInt();
    }

    public float getFloat(String key) {
        return getFloat(key, 0f);
    }

    public synchronized float getFloat(String key, float defValue) {
        BaseContainer c = data.get(key);
        return c == null ? defValue : c.toFloat();
    }

    public long getLong(String key) {
        return getLong(key, 0L);
    }

    public synchronized long getLong(String key, long defValue) {
        BaseContainer c = data.get(key);
        return c == null ? defValue : c.toLong();
    }

    public double getDouble(String key) {
        return getDouble(key, 0D);
    }

    public synchronized double getDouble(String key, double defValue) {
        BaseContainer c = data.get(key);
        return c == null ? defValue : c.toDouble();
    }

    public String getString(String key) {
        return getString(key, "");
    }

    public synchronized String getString(String key, String defValue) {
        BaseContainer container = data.get(key);
        if (container == null) {
            return defValue;
        }
        
        // 对于STRING类型且为外部文件，需要特殊处理
        if (container.getType() == DataType.STRING) {
            StringContainer c = (StringContainer) container;
            if (c.external) {
                String str = FileHelper.getStringFromFile(this, c, cipher);
                if (str == null || str.isEmpty()) {
                    remove(key);
                    return defValue;
                } else {
                    c.value = str;
                    c.external = false;
                    return str;
                }
            }
        }
        
        return container.toStringValue();
    }

    public byte[] getArray(String key) {
        return getArray(key, EMPTY_ARRAY);
    }

    public synchronized byte[] getArray(String key, byte[] defValue) {
        BaseContainer container = data.get(key);
        if (container == null || container.getType() != DataType.ARRAY) {
            return defValue;
        }
        ArrayContainer c = (ArrayContainer) container;
        if (c.external) {
            byte[] bytes = FileHelper.getArrayFromFile(this, c, cipher);
            if (bytes == null || bytes.length == 0) {
                remove(key);
                return defValue;
                        } else {
                c.value = bytes;
                c.external = false;
                return bytes;
                    }
                } else {
            return (byte[]) c.value;
        }
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> T getObject(String key) {
        BaseContainer container = data.get(key);
        if (container == null || container.getType() != DataType.OBJECT) {
            return null;
        }
        ObjectContainer c = (ObjectContainer) container;
        if (c.external) {
            Object obj = FileHelper.getObjectFromFile(this, c, cipher);
            if (obj == null) {
                remove(key);
                return null;
            } else {
                c.value = obj;
                c.external = false;
                return (T) obj;
            }
        } else {
            return (T) c.value;
        }
    }

    public synchronized Set<String> getStringSet(String key) {
        return getObject(key);
    }

    @Nullable
    @Override
    public Set<String> getStringSet(String key, @Nullable Set<String> defValues) {
        Set<String> set = getStringSet(key);
        return set != null ? set : defValues;
    }

    @Override
    public Editor edit() {
        return this;
    }

    @Override
    public synchronized Map<String, Object> getAll() {
        int size = data.size();
        if (size == 0) {
            return new HashMap<>();
        }
        Map<String, Object> result = new HashMap<>(size * 4 / 3 + 1);
        for (Map.Entry<String, BaseContainer> entry : data.entrySet()) {
            String key = entry.getKey();
            BaseContainer c = entry.getValue();
            Object value = null;
            switch (c.getType()) {
                case DataType.BOOLEAN:
                    value = ((BooleanContainer) c).value;
                    break;
                case DataType.INT:
                    value = ((IntContainer) c).value;
                    break;
                case DataType.FLOAT:
                    value = ((FloatContainer) c).value;
                    break;
                case DataType.LONG:
                    value = ((LongContainer) c).value;
                    break;
                case DataType.DOUBLE:
                    value = ((DoubleContainer) c).value;
                    break;
                case DataType.STRING:
                    StringContainer sc = (StringContainer) c;
                    value = sc.external ? FileHelper.getStringFromFile(this, sc, cipher) : sc.value;
                    break;
                case DataType.ARRAY:
                    ArrayContainer ac = (ArrayContainer) c;
                    value = ac.external ? FileHelper.getArrayFromFile(this, ac, cipher) : ac.value;
                    break;
                case DataType.OBJECT:
                    ObjectContainer oc = (ObjectContainer) c;
                    value = oc.external ? FileHelper.getObjectFromFile(this, oc, cipher) : ((ObjectContainer) c).value;
                    break;
            }
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    // ==================== 数据写入和删除方法 (Put/Remove Methods) ====================

    public synchronized Editor remove(String key) {
        if (closed) return this;
        BaseContainer container = data.get(key);
        if (container != null) {
            final String oldFileName;
            data.remove(key);
            byte type = container.getType();
            if (type <= DataType.DOUBLE) {
                int keySize = FastBuffer.getStringSize(key);
                int start = container.offset - (2 + keySize);
                remove(type, start, container.offset + TYPE_SIZE[type]);
                oldFileName = null;
            } else {
                VarContainer c = (VarContainer) container;
                remove(type, c.start, c.offset + c.valueSize);
                oldFileName = c.external ? (String) c.value : null;
            }
            byte newByte = (byte) (type | DataType.DELETE_MASK);
            if (writingMode == NON_BLOCKING) {
                aBuffer.putLong(4, checksum);
                aBuffer.put(removeStart, newByte);
                bBuffer.putLong(4, checksum);
                bBuffer.put(removeStart, newByte);
            } else {
                fastBuffer.putLong(4, checksum);
            }
            removeStart = 0;
            if (oldFileName != null) {
                if (writingMode == NON_BLOCKING) {
                    FileHelper.deleteExternalFile(this, oldFileName);
                } else {
                    deletedFiles.add(oldFileName);
                }
            }
            GCHelper.checkGC(this);
            checkIfCommit();
        }
        return this;
    }

    public synchronized Editor clear() {
        if (closed) return this;
        FileHelper.clearData(this);
        if (writingMode != NON_BLOCKING) {
            FileHelper.deleteCFiles(this);
        }
        notifyListeners(null);
        return this;
    }

    /**
     * 批量保存。
     * 仅支持 [boolean, int, long, float, double, String, byte[], Set of String] 类型和带编码器的对象。
     *
     * @param values   键值映射
     * @param encoders 值类型到编码器的映射
     */
    public synchronized void putAll(Map<String, Object> values, Map<Class, FastEncoder> encoders) {
        if (closed) return;
        if (writingMode != NON_BLOCKING) {
            autoCommit = false;
        }
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key != null && !key.isEmpty()) {
                if (value instanceof String) {
                    putString(key, (String) value);
                } else if (value instanceof Boolean) {
                    putBoolean(key, (Boolean) value);
                } else if (value instanceof Integer) {
                    putInt(key, (Integer) value);
                } else if (value instanceof Long) {
                    putLong(key, (Long) value);
                } else if (value instanceof Float) {
                    putFloat(key, (Float) value);
                } else if (value instanceof Double) {
                    putDouble(key, (Double) value);
                } else if (value instanceof byte[]) {
                    putArray(key, (byte[]) value);
                } else {
                    encodeObject(key, value, encoders);
                }
            }
        }
        if (writingMode != NON_BLOCKING) {
            commit();
        }
    }

    public void putAll(Map<String, Object> values) {
        putAll(values, null);
    }

    private void encodeObject(String key, Object value, Map<Class, FastEncoder> encoders) {
        if (value instanceof Set) {
            Set set = (Set) value;
            if (set.isEmpty() || set.iterator().next() instanceof String) {
                //noinspection unchecked
                putStringSet(key, set);
                return;
            }
        }
        if (encoders != null) {
            FastEncoder encoder = encoders.get(value.getClass());
            if (encoder != null) {
                //noinspection unchecked
                putObject(key, value, encoder);
            } else {
                LoggerHelper.warning(this, new Exception("missing encoder for type:" + value.getClass()));
            }
        } else {
            LoggerHelper.warning(this, new Exception("missing encoders"));
        }
    }

    /**
     * 强制将任何更改写入包含映射文件的存储设备。
     * 除非写入的内容非常重要，否则无需调用此方法。
     * 在数据同步到磁盘之前系统崩溃或断电可能导致最近的更新丢失。
     */
    public synchronized void force() {
        FileHelper.force(this);
    }

    /**
     * 当使用 SYNC_BLOCKING 或 ASYNC_BLOCKING 模式打开文件时，
     * 默认情况下会在每次 put 或 remove 后自动提交。
     * 如果需要批量更新多个键值，可以先调用此方法，
     * 然后在更新后调用 {@link #commit()}，该方法将再次将 {@link #autoCommit} 恢复为 'true'。
     */
    public synchronized void disableAutoCommit() {
        this.autoCommit = false;
    }

    public synchronized boolean commit() {
        if (closed) return false;
        autoCommit = true;
        return commitToCFile();
    }

    @Override
    public synchronized void apply() {
        if (closed) return;
        autoCommit = true;
        commitToCFile();
    }

    private void handleChange(String key) {
        checkIfCommit();
        notifyListeners(key);
    }

    private void checkIfCommit() {
        if (writingMode != NON_BLOCKING && autoCommit) {
            commitToCFile();
        }
    }

    private boolean commitToCFile() {
        if (writingMode == ASYNC_BLOCKING) {
            applyExecutor.execute(() -> FileHelper.writeToCFile(this));
        } else if (writingMode == SYNC_BLOCKING) {
            return FileHelper.writeToCFile(this);
        }
        return true;
    }

    private synchronized void notifyListeners(String key) {
        if (listeners.isEmpty()) return;
        for (SharedPreferences.OnSharedPreferenceChangeListener listener : listeners) {
            mainHandler.post(() -> listener.onSharedPreferenceChanged(this, key));
        }
    }

    @Override
    public synchronized void registerOnSharedPreferenceChangeListener(
            OnSharedPreferenceChangeListener listener) {
        if (listener == null) {
            return;
        }
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    @Override
    public synchronized void unregisterOnSharedPreferenceChangeListener(
            OnSharedPreferenceChangeListener listener) {
        listeners.remove(listener);
    }

    public synchronized Editor putBoolean(String key, boolean value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.BOOLEAN) {
            remove(key);
            container = null;
        }
        BooleanContainer c = (BooleanContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.BOOLEAN)) return this;
            int offset = fastBuffer.position;
            fastBuffer.put((byte) (value ? 1 : 0));
            updateChange();
            data.put(key, new BooleanContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            c.value = value;
            updateBoolean((byte) (value ? 1 : 0), c.offset);
            handleChange(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putInt(String key, int value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.INT) {
            remove(key);
            container = null;
        }
        IntContainer c = (IntContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.INT)) return this;
            int offset = fastBuffer.position;
            fastBuffer.putInt(cipher != null ? cipher.encrypt(value) : value);
            updateChange();
            data.put(key, new IntContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            int newValue = cipher != null ? cipher.encrypt(value) : value;
            int oldValue = cipher != null ? fastBuffer.getInt(c.offset) : c.value;
            long sum = (newValue ^ oldValue) & 0xFFFFFFFFL;
            c.value = value;
            updateInt32(newValue, sum, c.offset);
            handleChange(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putFloat(String key, float value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.FLOAT) {
            remove(key);
            container = null;
        }
        FloatContainer c = (FloatContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.FLOAT)) return this;
            int offset = fastBuffer.position;
            fastBuffer.putInt(getNewFloatValue(value));
            updateChange();
            data.put(key, new FloatContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            int newValue = getNewFloatValue(value);
            int oldValue = fastBuffer.getInt(c.offset);
            long sum = (newValue ^ oldValue) & 0xFFFFFFFFL;
            c.value = value;
            updateInt32(newValue, sum, c.offset);
            handleChange(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putLong(String key, long value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.LONG) {
            remove(key);
            container = null;
        }
        LongContainer c = (LongContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.LONG)) return this;
            int offset = fastBuffer.position;
            fastBuffer.putLong(cipher != null ? cipher.encrypt(value) : value);
            updateChange();
            data.put(key, new LongContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            long newValue = cipher != null ? cipher.encrypt(value) : value;
            long oldValue = cipher != null ? fastBuffer.getLong(c.offset) : c.value;
            long sum = newValue ^ oldValue;
            c.value = value;
            updateInt64(newValue, sum, c.offset);
            handleChange(key);
        }
        return this;
    }

    public synchronized Editor putDouble(String key, double value) {
        if (closed) return this;
        checkKey(key);
        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.DOUBLE) {
            remove(key);
            container = null;
        }
        DoubleContainer c = (DoubleContainer) container;
        if (c == null) {
            if (!wrapHeader(key, DataType.DOUBLE)) return this;
            int offset = fastBuffer.position;
            fastBuffer.putLong(getNewDoubleValue(value));
            updateChange();
            data.put(key, new DoubleContainer(offset, value));
            handleChange(key);
        } else if (c.value != value) {
            long newValue = getNewDoubleValue(value);
            long oldValue = fastBuffer.getLong(c.offset);
            long sum = newValue ^ oldValue;
            c.value = value;
            updateInt64(newValue, sum, c.offset);
            handleChange(key);
        }
        return this;
    }

    @Override
    public synchronized Editor putString(String key, String value) {
        if (closed) return this;
        checkKey(key);
        if (value == null) {
            remove(key);
                } else {
            BaseContainer container = data.get(key);
            if (container != null && container.getType() != DataType.STRING) {
                remove(key);
                container = null;
            }
            StringContainer c = (StringContainer) container;
            if (c != null && !c.external && value.equals(c.value)) {
                return this;
            }
            byte[] bytes = value.isEmpty() ? EMPTY_ARRAY : value.getBytes(StandardCharsets.UTF_8);
            byte[] newBytes = cipher != null ? cipher.encrypt(bytes) : bytes;
            if (newBytes == null) {
                LoggerHelper.error(this, new Exception(ENCRYPT_FAILED));
                return this;
            }
            addOrUpdate(key, value, newBytes, c, DataType.STRING);
            handleChange(key);
        }
        return this;
    }

    public synchronized Editor putArray(String key, byte[] value) {
        if (closed) return this;
        checkKey(key);
        if (value == null) {
            remove(key);
        } else {
            BaseContainer container = data.get(key);
            if (container != null && container.getType() != DataType.ARRAY) {
                remove(key);
                container = null;
            }
            ArrayContainer c = (ArrayContainer) container;
            byte[] newBytes = cipher != null ? cipher.encrypt(value) : value;
            if (newBytes == null) {
                LoggerHelper.error(this, new Exception(ENCRYPT_FAILED));
                return this;
            }
            addOrUpdate(key, value, newBytes, c, DataType.ARRAY);
            handleChange(key);
        }
        return this;
    }

    /**
     * @param key     key
     * @param value   新值
     * @param encoder 将 value encode 为 byte[] 的编码器。
     *                编码器必须在 Builder.encoder() 中注册，以便在下次加载时将 byte[] decode 为对象。
     * @param <T>     值的类型
     */
    public synchronized <T> void putObject(String key, T value, FastEncoder<T> encoder) {
        if (closed) return;
        checkKey(key);
        if (encoder == null) {
            throw new IllegalArgumentException("Encoder is null");
        }
        String tag = encoder.tag();
        if (tag == null || tag.isEmpty() || tag.length() > 50) {
            throw new IllegalArgumentException("Invalid encoder tag:" + tag);
        }
        if (!encoderMap.containsKey(tag)) {
            throw new IllegalArgumentException("Encoder hasn't been registered");
        }

        if (value == null) {
            remove(key);
            return;
        }
        byte[] objBytes = null;
        try {
            objBytes = encoder.encode(value);
            } catch (Exception e) {
            LoggerHelper.error(this, e);
        }
        if (objBytes == null) {
            remove(key);
            return;
        }

        BaseContainer container = data.get(key);
        if (container != null && container.getType() != DataType.OBJECT) {
            remove(key);
            container = null;
        }
        ObjectContainer c = (ObjectContainer) container;

        // 组装对象字节
        int tagSize = FastBuffer.getStringSize(tag);
        FastBuffer buffer = new FastBuffer(1 + tagSize + objBytes.length);
        buffer.put((byte) tagSize);
        buffer.putString(tag);
        buffer.putBytes(objBytes);
        byte[] bytes = buffer.hb;

        byte[] newBytes = cipher != null ? cipher.encrypt(bytes) : bytes;
        if (newBytes == null) return;
        addOrUpdate(key, value, newBytes, c, DataType.OBJECT);
        handleChange(key);
    }

    public synchronized Editor putStringSet(String key, Set<String> set) {
        if (closed) return this;
        if (set == null) {
            remove(key);
            } else {
            putObject(key, set, StringSetEncoder.INSTANCE);
        }
        return this;
    }

    // ==================== Put操作辅助方法 ====================
    
    private void preparePutBytes() {
        GCHelper.ensureSize(this, updateSize);
        updateStart = dataEnd;
        dataEnd += updateSize;
        fastBuffer.position = updateStart;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean wrapHeader(String key, byte type) {
        return wrapHeader(key, type, TYPE_SIZE[type]);
    }

    private boolean wrapHeader(String key, byte type, int valueSize) {
        if (cipher != null) {
            byte[] keyBytes = cipher.encrypt(key.getBytes(StandardCharsets.UTF_8));
            if (keyBytes == null) {
                LoggerHelper.error(this, new Exception(ENCRYPT_FAILED));
                return false;
            }
            int keySize = keyBytes.length;
            prepareHeaderInfo(keySize, valueSize, type);
            fastBuffer.put((byte) keySize);
            System.arraycopy(keyBytes, 0, fastBuffer.hb, fastBuffer.position, keySize);
            fastBuffer.position += keySize;
        } else {
            int keySize = FastBuffer.getStringSize(key);
            prepareHeaderInfo(keySize, valueSize, type);
            wrapKey(key, keySize);
        }
        return true;
    }

    private void prepareHeaderInfo(int keySize, int valueSize, byte type) {
        if (keySize > 0xFF) {
            throw new IllegalArgumentException("key's length must less than 256");
        }
        updateSize = 2 + keySize + valueSize;
        preparePutBytes();
        fastBuffer.put(type);
    }

    private void wrapKey(String key, int keySize) {
        fastBuffer.put((byte) keySize);
        if (keySize == key.length()) {
            //noinspection deprecation
            key.getBytes(0, keySize, fastBuffer.hb, fastBuffer.position);
            fastBuffer.position += keySize;
        } else {
            fastBuffer.putString(key);
        }
    }

    private void addOrUpdate(String key, Object value, byte[] bytes, VarContainer c, byte type) {
        if (c == null) {
            addObject(key, value, bytes, type);
        } else {
            if (!c.external && c.valueSize == bytes.length) {
                updateBytes(c.offset, bytes);
                c.value = value;
            } else {
                updateObject(key, value, bytes, c);
            }
        }
    }

    private void addObject(String key, Object value, byte[] bytes, byte type) {
        int offset = wrapArray(key, bytes, type);
        if (offset > 0) {
            int size = bytes.length;
            BaseContainer c;
            if (type == DataType.STRING) {
                c = new StringContainer(updateStart, offset, (String) value, size, false);
            } else if (type == DataType.ARRAY) {
                c = new ArrayContainer(updateStart, offset, value, size, false);
            } else {
                c = new ObjectContainer(updateStart, offset, value, size, false);
            }
            data.put(key, c);
            updateChange();
        }
    }

    private void updateObject(String key, Object value, byte[] bytes, VarContainer c) {
        int offset = wrapArray(key, bytes, c.getType());
        if (offset > 0) {
            String oldFileName = c.external ? (String) c.value : null;
            remove(c.getType(), c.start, c.offset + c.valueSize);
            c.start = updateStart;
            c.offset = offset;
            c.external = false;
            c.value = value;
            c.valueSize = bytes.length;
            updateChange();
            GCHelper.checkGC(this);
            if (oldFileName != null) {
                removeOldFile(oldFileName);
            }
        }
    }

    /**
     * 保存成功时返回偏移量；
     * 保存失败时返回 0 (仅加密失败时会返回0）
     */
    private int wrapArray(String key, byte[] value, byte type) {
        // 根据数据大小选择合适的类型和长度编码方式
        boolean isLarge = value.length >= 0xFFFF;
        byte actualType = isLarge ? getLargeType(type) : type;
        int lengthSize = isLarge ? 4 : 2;
        
        if (!wrapHeader(key, actualType, lengthSize + value.length)) {
            return 0;
        }
        
        if (isLarge) {
            fastBuffer.putInt(value.length);
        } else {
            fastBuffer.putShort((short) value.length);
        }
        
        int offset = fastBuffer.position;
        fastBuffer.putBytes(value);
        return offset;
    }
    
    private byte getLargeType(byte type) {
        switch (type) {
            case DataType.STRING: return DataType.STRING_LARGE;
            case DataType.ARRAY: return DataType.ARRAY_LARGE;
            case DataType.OBJECT: return DataType.OBJECT_LARGE;
            default: return type;
        }
    }

    private int getNewFloatValue(float value) {
        int intValue = Float.floatToRawIntBits(value);
        return cipher != null ? cipher.encrypt(intValue) : intValue;
    }

    private long getNewDoubleValue(double value) {
        long longValue = Double.doubleToRawLongBits(value);
        return cipher != null ? cipher.encrypt(longValue) : longValue;
    }

    /**
     * 关闭 kv 实例。<br>
     * 如果 kv 已关闭，它将不接受任何更新。<br>
     * 如果 kv 被缓存，调用此方法后, 请记得将其从缓存中移除。
     */
    public synchronized void close() {
        FileHelper.close(this);
        synchronized (Builder.class) {
            Builder.INSTANCE_MAP.remove(path + name);
        }
    }

    private void updateChange() {
        checksum ^= fastBuffer.getChecksum(updateStart, updateSize);
        int packedSize = packSize(dataEnd - DATA_START);
        if (writingMode == NON_BLOCKING) {
            // 当更改数据的大小超过 8 字节时,checksum 可能在小概率下无法检查完整性。
            // 因此，我们在写入数据之前，将 dataLen 设置为负数；
            // 如果在向 mmap 内存写入数据时发生崩溃，我们可以根据dataLen为负数知道写入没有完成。
            aBuffer.putInt(0, -1);
            syncToABBuffer(aBuffer);
            aBuffer.putInt(0, packedSize);

            // bBuffer 不需要在写入字节之前标记 dataLen 部分，
            // 因为此时 aBuffer 已经完全写入了。
            // 我们只需在任何时候至少有一个文件是完整的即可。
            bBuffer.putInt(0, packedSize);
            syncToABBuffer(bBuffer);
        } else {
            fastBuffer.putInt(0, packedSize);
            fastBuffer.putLong(4, checksum);
        }
        removeStart = 0;
        updateSize = 0;
    }

    private void syncToABBuffer(MappedByteBuffer buffer) {
        buffer.putLong(4, checksum);
        if (removeStart != 0) {
            buffer.put(removeStart, fastBuffer.hb[removeStart]);
        }
        if (updateSize != 0) {
            buffer.position(updateStart);
            buffer.put(fastBuffer.hb, updateStart, updateSize);
        }
    }

    private void updateBoolean(byte value, int offset) {
        checksum ^= BufferHelper.shiftCheckSum(1L, offset);
        if (writingMode == NON_BLOCKING) {
            aBuffer.putLong(4, checksum);
            aBuffer.put(offset, value);
            bBuffer.putLong(4, checksum);
            bBuffer.put(offset, value);
        } else {
            fastBuffer.putLong(4, checksum);
        }
        fastBuffer.hb[offset] = value;
    }

    private void updateInt32(int value, long sum, int offset) {
        checksum ^= BufferHelper.shiftCheckSum(sum, offset);
        if (writingMode == NON_BLOCKING) {
            aBuffer.putLong(4, checksum);
            aBuffer.putInt(offset, value);
            bBuffer.putLong(4, checksum);
            bBuffer.putInt(offset, value);
        } else {
            fastBuffer.putLong(4, checksum);
        }
        fastBuffer.putInt(offset, value);
    }

    private void updateInt64(long value, long sum, int offset) {
        checksum ^= BufferHelper.shiftCheckSum(sum, offset);
        if (writingMode == NON_BLOCKING) {
            aBuffer.putLong(4, checksum);
            aBuffer.putLong(offset, value);
            bBuffer.putLong(4, checksum);
            bBuffer.putLong(offset, value);
        } else {
            fastBuffer.putLong(4, checksum);
        }
        fastBuffer.putLong(offset, value);
    }

    private void updateBytes(int offset, byte[] bytes) {
        int size = bytes.length;
        checksum ^= fastBuffer.getChecksum(offset, size);
        fastBuffer.position = offset;
        fastBuffer.putBytes(bytes);
        checksum ^= fastBuffer.getChecksum(offset, size);
        
        if (writingMode == NON_BLOCKING) {
            aBuffer.putInt(0, -1);
            aBuffer.putLong(4, checksum);
            aBuffer.position(offset);
            aBuffer.put(bytes);
            aBuffer.putInt(0, packSize(dataEnd - DATA_START));
            bBuffer.putLong(4, checksum);
            bBuffer.position(offset);
            bBuffer.put(bytes);
        } else {
            fastBuffer.putLong(4, checksum);
        }
    }

    private void removeOldFile(String oldFileName) {
        if (writingMode == NON_BLOCKING) {
            FileHelper.deleteExternalFile(this, oldFileName);
        } else {
            deletedFiles.add(oldFileName);
        }
    }

    private void remove(byte type, int start, int end) {
        GCHelper.countInvalid(this, start, end);
        byte newByte = (byte) (type | DataType.DELETE_MASK);
        byte oldByte = fastBuffer.hb[start];
        int shift = (start & 7) << 3;
        checksum ^= ((long) (newByte ^ oldByte) & 0xFF) << shift;
        fastBuffer.hb[start] = newByte;
        removeStart = start;
    }

    private void checkKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is empty");
        }
    }

    // ==================== Builder模式和静态方法 ====================

    public static final class Builder {
        static final Map<String, FastKV> INSTANCE_MAP = new ConcurrentHashMap<>();
        private final String path;
        private final String name;
        private FastEncoder[] encoders;
        private FastCipher cipher;
        private int writingMode = NON_BLOCKING;

        public Builder(Context context, String name) {
            if (context == null) {
                throw new IllegalArgumentException("context is null");
            }
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name is empty");
            }
            this.path = context.getFilesDir().getAbsolutePath() + "/fastkv/";
            this.name = name;
        }

        public Builder(String path, String name) {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("path is empty");
            }
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name is empty");
            }
            this.path = path.endsWith("/") ? path : (path + '/');
            this.name = name;
        }

        /**
         * 设置对象编码器
         *
         * @param encoders 用于将字节解码为对象的编码器数组。
         * @return 构建器
         */
        public Builder encoder(FastEncoder[] encoders) {
            this.encoders = encoders;
            return this;
        }

        /**
         * 设置加密密码器。
         */
        public Builder cipher(FastCipher cipher) {
            this.cipher = cipher;
            return this;
        }

        /**
         * 将写入模式设置为 SYNC_BLOCKING。
         * <p>
         * 在非阻塞模式下（使用 mmap 写入数据），
         * 如果系统在将数据刷新到磁盘之前崩溃或断电，可能会丢失更新。
         * 可使用 {@link #force()} 避免丢失更新，或使用 SYNC_BLOCKING 模式。
         * <p>
         * 在阻塞模式下，每次更新都会将所有数据写入文件，这是昂贵的成本。
         * <p>
         * 因此建议仅在数据非常重要时才使用阻塞模式。
         * <p>
         *
         * @return 构建器
         */
        public Builder blocking() {
            writingMode = SYNC_BLOCKING;
            return this;
        }

        /**
         * 类似于 {@link #blocking()}，但将写入任务放到异步线程。
         *
         * @return 构建器
         */
        public Builder asyncBlocking() {
            writingMode = ASYNC_BLOCKING;
            return this;
        }

        public FastKV build() {
            String key = path + name;
            FastKV kv = INSTANCE_MAP.get(key);
            if (kv == null) {
                synchronized (Builder.class) {
                    kv = INSTANCE_MAP.get(key);
                    if (kv == null) {
                        kv = new FastKV(path, name, encoders, cipher, writingMode);
                        INSTANCE_MAP.put(key, kv);
                    }
                }
            }
            return kv;
        }
    }

    /**
     * 适配旧的 SharePreferences，
     * 返回一个使用 FastKV 存储策略的新 SharedPreferences。
     * <p>
     * 注意：旧的 SharePreferences 必须实现 getAll() 方法，
     * 否则无法将旧数据导入新文件。
     *
     * @param context       上下文
     * @param name          SharePreferences 的名称
     * @return FastKV 的包装器，实现了 SharePreferences。
     */
    public static SharedPreferences adapt(Context context, String name) {
        String path = context.getFilesDir().getAbsolutePath() + "/fastkv";
        FastKV kv = new FastKV.Builder(path, name).build();
        final String flag = "kv_import_flag";
        if (!kv.contains(flag)) {
            SharedPreferences oldPreferences = context.getSharedPreferences(name, Context.MODE_PRIVATE);
            //noinspection unchecked
            Map<String, Object> allData = (Map<String, Object>) oldPreferences.getAll();
            kv.putAll(allData);
            kv.putBoolean(flag, true);
        }
        return kv;
    }

    @NonNull
    @Override
    public String toString() {
        return "FastKV: path:" + path + " name:" + name;
    }
}
