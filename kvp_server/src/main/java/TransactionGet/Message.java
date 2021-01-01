package TransactionGet;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Message {

    final long id;
    final Set<Long> keys;
    final Map<Long, byte[]> values;

    private Message(long id, Set<Long> keys){
        this.id = id;
        this.keys = keys;
        this.values = null;
    }

    private Message(long id, Map<Long, byte[]> values){
        this.id = id;
        this.keys = null;
        this.values = values;
    }

    static byte[] encode(long id, Set<Long> keys){
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + Long.BYTES * keys.size());
        buf.putLong(id);
        buf.putInt(keys.size());
        for(Long key : keys)
            buf.putLong(key);
        return buf.array();
    }

    static byte[] encode(long id, Map<Long, byte[]> values){
        byte[] map = TransactionMap.encode(values);
        return ByteBuffer.allocate(Long.BYTES + map.length)
                .putLong(id).put(map).array();
    }

    static Message decodeKeys(ByteBuffer buf){
        long id = buf.getLong();
        Set<Long> keys = new HashSet<>();
        int size = buf.getInt();
        for(int i=0; i<size; i++)
            keys.add(buf.getLong());
        return new Message(id, keys);
    }

    static Message decodeValues(ByteBuffer buf){
        long id = buf.getLong();
        return new Message(id, TransactionMap.decode(buf));
    }

    @Override
    public String toString() {
        return "M{" + id + " k=" + keys + " v=" + values + '}';
    }
}
