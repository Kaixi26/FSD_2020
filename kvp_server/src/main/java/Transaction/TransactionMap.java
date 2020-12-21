package Transaction;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TransactionMap {
    public static byte[] encode(Map<Long, byte[]> values){
        int size = Integer.BYTES + (Integer.BYTES + Long.BYTES) * values.size();
        for(byte[] bytes : values.values()) size += bytes.length;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(values.size());
        for(Map.Entry<Long, byte[]> entry : values.entrySet()){
            buf.putLong(entry.getKey());
            buf.putInt(entry.getValue().length);
            buf.put(entry.getValue());
        }
        return buf.array();
    }

    public static Map<Long, byte[]> decode(ByteBuffer buf){
        int size = buf.getInt();
        Map<Long, byte[]> values = new HashMap<>();
        for(int i=0; i<size; i++){
            long key = buf.getLong();
            int len = buf.getInt();
            byte[] value = new byte[len];
            buf.get(value);
            values.put(key, value);
        }
        return values;
    }
}
