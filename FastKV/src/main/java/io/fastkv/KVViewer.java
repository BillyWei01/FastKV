package io.fastkv;


import java.util.Map;

public class KVViewer {
    public static void printFastKV(FastKV kv) {
        try {
            Map<String, Object> map = kv.getAll();
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if ((value instanceof String) && ((String) value).length() > 50) {
                    String v = (String) value;
                    builder.append(key).append(" = ").append(v.substring(0, 50)).append(" size: ").append(v.length());
                } else if (value instanceof byte[]) {
                    byte[] v = (byte[]) value;
                    builder.append(key).append(" = ").append(" array of size: ").append(v.length);
                } else {
                    builder.append(key).append(" = ").append(value);
                }
                builder.append('\n');
            }
            System.out.println(builder);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
