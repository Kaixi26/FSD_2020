package Client;

import Serialization.KeyValueMap;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class Message {
    static final private byte MAP = 0;
    static final private byte GET = 1;
    final private byte type;
    private Map<Long, byte[]> values = null;
    private Collection<Long> keys = null;

    Message(Map<Long, byte[]> values){
        this.type = MAP;
        this.values = values;
    }

    Message(Collection<Long> keys){
        this.type = GET;
        this.keys = keys;
    }

    @Nullable
    public Map<Long, byte[]> getValues() {
        return values;
    }

    @Nullable
    public Collection<Long> getKeys() {
        return keys;
    }

    byte[] encode(){
        ByteBuffer buffer;
        if(type == MAP) {
            byte[] encodedMap = KeyValueMap.encode(values);
            buffer = ByteBuffer.allocate(Byte.BYTES + encodedMap.length);
            buffer.put(MAP);
            buffer.put(encodedMap);
        } else {
            buffer = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + Long.BYTES * keys.size());
            buffer.put(GET);
            buffer.putInt(keys.size());
            for(long key : keys) buffer.putLong(key);
        }
        return buffer.array();
    }

    static Message decode(ByteBuffer buffer){
        switch (buffer.get()){
            case MAP:
                return new Message(KeyValueMap.decode(buffer));
            case GET:
                Collection<Long> keys = new ArrayList<>();
                int size = buffer.getInt();
                for(int i=0; i<size; i++) keys.add(buffer.getLong());
                return new Message(keys);
            default:
                return null;
        }
    }

    @Override
    public String toString() {
        return "v=" + values + ", k=" + keys;
    }
}
