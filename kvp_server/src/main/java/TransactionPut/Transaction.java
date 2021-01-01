package TransactionPut;

import io.atomix.utils.net.Address;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Transaction {

    final int local_id;
    final long id;
    Set<Address> missingPeers = new HashSet<>();
    Set<Long> keys;
    CompletableFuture<Void> completedTransaction = new CompletableFuture<>();

    private Transaction(long id, Set<Long> keys, int local_id){
        this.id = id;
        this.local_id = local_id;
        this.keys = keys;
    }

    Transaction(long id, Collection<Address> peers, Set<Long> keys, int local_id){
        this.id = id;
        this.local_id = local_id;
        missingPeers = new HashSet<>(peers);
        this.keys = keys;
    }

    byte[] encode() {

        //byte[] encodedMap = TransactionMap.encode(values);
        //ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + encodedMap.length);
        //buf.putLong(id);
        //buf.put(encodedMap);
        //return buf.array();
        ByteBuffer buf = ByteBuffer.allocate(
                Integer.BYTES + Long.BYTES + Integer.BYTES +
                keys.size() * Long.BYTES);
        buf.putInt(local_id);
        buf.putLong(id);
        buf.putInt(keys.size());
        for(Long key : keys)
            buf.putLong(key);
        return buf.array();
    }

    static Transaction decode(ByteBuffer buf){
        //long id = buf.getLong();
        //Map<Long, byte[]> values = TransactionMap.decode(buf);
        //return new Transaction(id, values);
        Set<Long> keys = new HashSet<>();
        int local_id = buf.getInt();
        long id = buf.getLong();
        long size = buf.getInt();
        for(int i=0; i<size; i++)
            keys.add(buf.getLong());
        return new Transaction(id, keys, local_id);
    }

    @Override
    public String toString() {
        return local_id + "[" + id + "->" + Arrays.toString(keys.toArray()) + "]";
    }
}
