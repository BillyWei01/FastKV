package io.fastkv;

import androidx.annotation.NonNull;

/**
 * 内存段，用于垃圾回收过程中标记无效的数据段。
 * 包含起始位置和结束位置，支持按起始位置排序。
 */
class Segment implements Comparable<Segment> {
    int start;
    int end;

    Segment(int start, int end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public int compareTo(Segment o) {
        return start - o.start;
    }

    @NonNull
    @Override
    public String toString() {
        return "Segment{" + "start=" + start + ", end=" + end + '}';
    }
} 