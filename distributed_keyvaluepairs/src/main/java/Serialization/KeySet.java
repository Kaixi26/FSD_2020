package Serialization;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KeySet {

    public static byte[] encode(Set<Long> keys){
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Long.BYTES * keys.size())
                .putInt(keys.size());
        keys.forEach(buf::putLong);
        return buf.array();
    }

    public static Set<Long> decode(ByteBuffer buf){
        Set<Long> keys = new HashSet<>();
        int size = buf.getInt();
        for(int i=0; i<size; i++)
            keys.add(buf.getLong());
        return keys;
    }

}
