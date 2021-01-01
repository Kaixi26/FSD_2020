package TransactionPut;

import Database.KeyValue;
import Database.ValueOrServer;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Manager {

    final private String MESSAGETYPE_CONFIRM = "ConfirmPutTransaction";
    final private String MESSAGETYPE_TRANSACTION = "TransactionPut";

    final private int local_id;
    private Map<Integer, Address> peers;
    private long transactionId = 0;
    private KeyValue kvdb;
    private NettyMessagingService ms;
    private Map<Long, Transaction> transactions = new HashMap<>();
    private Lock lock = new ReentrantLock();

    public Manager(int local_id, Map<Integer, Address> peers, KeyValue kvdb, NettyMessagingService ms, ScheduledExecutorService es){
        this.local_id = local_id;
        this.kvdb = kvdb;
        this.peers = peers;
        this.ms = ms;
        ms.registerHandler(MESSAGETYPE_TRANSACTION, (a,m) -> {
            handleTransaction(a, Transaction.decode(ByteBuffer.wrap(m)));
        }, es);
        ms.registerHandler(MESSAGETYPE_CONFIRM, (a,m) -> {
            handleConfirm(a, ByteBuffer.wrap(m).getLong());
        }, es);
    }

    public CompletableFuture<Void> makeTransaction(Set<Long> keys){
        Transaction transaction;
        lock.lock();
        try {
            transaction = new Transaction(transactionId++, peers.values(), keys, local_id);
            transactions.put(transaction.id, transaction);
        } finally {
            lock.unlock();
        }
        byte[] buf = transaction.encode();
        for(Address peer : peers.values())
            ms.sendAsync(peer, MESSAGETYPE_TRANSACTION, buf);
        return transaction.completedTransaction;
    }

    void handleTransaction(Address address, Transaction transaction){
        byte[] buf = new byte[1];
        lock.lock();
        try {
            Set<Long> updatedKeys = transaction.keys;
            Map<Long, ValueOrServer> values = updatedKeys.stream().collect(Collectors.toMap(
                    Long::longValue,
                    _v -> new ValueOrServer(transaction.local_id)
            ));
            kvdb.put(values);
            buf = ByteBuffer.allocate(Long.BYTES).putLong(transaction.id).array();
        } finally {
            System.out.println("[Received]" + "[" + MESSAGETYPE_TRANSACTION + "|" + address + "]: " + transaction);
            lock.unlock();
        }
        ms.sendAsync(address , MESSAGETYPE_CONFIRM , buf);
    }

    void handleConfirm(Address address, Long id){
        try {
            Transaction transaction = transactions.get(id);
            transaction.missingPeers.remove(address);
            if (transaction.missingPeers.isEmpty()) {
                transaction.completedTransaction.complete((Void) null);
                transactions.remove(transaction.id);
            }
        } finally {
            System.out.println("[Received]" + "[" + MESSAGETYPE_CONFIRM + "|" + address + "]: " + id);
            lock.unlock();
        }
    }

}
