package io.fastkv;

import io.packable.PackDecoder;
import io.packable.PackEncoder;

import java.util.ArrayList;
import java.util.List;

public class LongListEncoder implements FastKV.Encoder<List<Long>> {
    public static final LongListEncoder INSTANCE = new LongListEncoder();

    private LongListEncoder() {
    }

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
