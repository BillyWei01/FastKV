package io.fastkv;

import io.fastkv.interfaces.FastLogger;

/**
 * 日志辅助类 - 统一管理FastKV的日志输出
 */
class LoggerHelper {
    /**
     * 记录错误信息
     */
    static void error(FastKV kv, String message) {
        FastLogger logger =  FastKVConfig.sLogger;
        if (logger != null) {
            logger.e(kv.name, new Exception(message));
        }
    }
    
    /**
     * 记录异常信息
     */
    static void error(FastKV kv, Exception e) {
        FastLogger logger =  FastKVConfig.sLogger;
        if (logger != null) {
            logger.e(kv.name, e);
        }
    }
    
    /**
     * 记录警告信息
     */
    static void warning(FastKV kv, Exception e) {
        FastLogger logger =  FastKVConfig.sLogger;
        if (logger != null) {
            logger.w(kv.name, e);
        }
    }
    
    /**
     * 记录信息
     */
    static void info(FastKV kv, String message) {
        FastLogger logger =  FastKVConfig.sLogger;
        if (logger != null) {
            logger.i(kv.name, message);
        }
    }
} 