package Transaction;

import Database.KeyValue;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Manager {

    final private String MESSAGETYPE_CONFIRM = "ConfirmTransaction";
    final private String MESSAGETYPE_TRANSACTION = "Transaction";

    private Collection<Address> peers;
    private long transactionId = 0;
    private KeyValue kvdb;
    private NettyMessagingService ms;
    private Map<Long, Transaction> transactions = new HashMap<>();
    private Lock lock = new ReentrantLock();

    public Manager(Collection<Address> peers, KeyValue kvdb, NettyMessagingService ms, ScheduledExecutorService es){
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

    public CompletableFuture<Void> makeTransaction(Map<Long, byte[]> values){
        Transaction transaction = new Transaction(transactionId++, peers, values);
        lock.lock();
        try {
            transactions.put(transaction.id, transaction);
        } finally {
            lock.unlock();
        }
        byte[] buf = transaction.encode();
        for(Address peer : peers)
            ms.sendAsync(peer, MESSAGETYPE_TRANSACTION, buf);
        return transaction.completedTransaction;
    }

    void handleTransaction(Address address, Transaction transaction){
        byte[] buf = new byte[1];
        lock.lock();
        try {
            kvdb.put(transaction.values);
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
