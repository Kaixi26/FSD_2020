package Serialization;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

public class KeyValueMap {

    static byte[] encodeEntry(Map.Entry<Long, byte[]> entry){
        return ByteBuffer.allocate(Long.BYTES + Integer.BYTES + Byte.BYTES * entry.getValue().length)
                .putLong(entry.getKey())
                .putInt(entry.getValue().length)
                .put(entry.getValue()).array();
    }

    static Map.Entry<Long, byte[]> decodeEntry(ByteBuffer buf){
        long key = buf.getLong();
        byte[] value = new byte[buf.getInt()];
        buf.get(value);
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static byte[] encode(Map<Long, byte[]> map){
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(ByteBuffer.allocate(Integer.BYTES).putInt(map.size()).array(), 0, Integer.BYTES);
        map.entrySet().stream().map(KeyValueMap::encodeEntry)
                .forEach(bytes -> buf.write(bytes, 0, bytes.length));
        return buf.toByteArray();
    }

    public static Map<Long, byte[]> decode(ByteBuffer buf){
        Map<Long, byte[]> ret = new HashMap<>();
        int size = buf.getInt();
        for(int i=0; i<size; i++) {
            Map.Entry<Long, byte[]> entry = decodeEntry(buf);
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }
}
