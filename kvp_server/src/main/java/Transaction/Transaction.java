package Transaction;

import io.atomix.utils.net.Address;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Transaction {

    final long id;
    Set<Address> missingPeers = new HashSet<>();
    Map<Long, byte[]> values;
    CompletableFuture<Void> completedTransaction = new CompletableFuture<>();

    private Transaction(long id, Map<Long, byte[]> values){
        this.id = id;
        this.values = values;
    }

    Transaction(long id, Collection<Address> peers, Map<Long, byte[]> values){
        this.id = id;
        missingPeers = new HashSet<>(peers);
        this.values = values;
    }

    byte[] encode() {

        byte[] encodedMap = TransactionMap.encode(values);
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + encodedMap.length);
        buf.putLong(id);
        buf.put(encodedMap);
        return buf.array();
    }

    static Transaction decode(ByteBuffer buf){
        long id = buf.getLong();
        Map<Long, byte[]> values = TransactionMap.decode(buf);
        return new Transaction(id, values);
    }

    @Override
    public String toString() {
        return id + "->" + Arrays.toString(values.entrySet().toArray());
    }
}
