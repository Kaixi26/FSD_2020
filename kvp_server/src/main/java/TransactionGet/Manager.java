package TransactionGet;

import Database.KeyValue;
import Database.ValueOrServer;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Manager {

    final private String MESSAGETYPE_CONFIRM = "ConfirmGetTransaction";
    final private String MESSAGETYPE_TRANSACTION = "TransactionGet";

    final int local_id;
    final Map<Integer, Address> peers;
    final Map<Address, Integer> peers_rev;
    final NettyMessagingService ms;
    final Lock lock = new ReentrantLock();
    final KeyValue kvp;
    private Map<Long, TransactionGet.Transaction> transactions = new HashMap<>();

    private long transactionId = 0;

    public Manager(int local_id, KeyValue kvp, Map<Integer, Address> peers, NettyMessagingService ms, ScheduledExecutorService es){
        this.local_id = local_id;
        this.peers = peers;
        this.peers_rev = peers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        this.ms = ms;
        this.kvp = kvp;
        ms.registerHandler(MESSAGETYPE_TRANSACTION, (a,m) -> {
            handleTransaction(a, Message.decodeKeys(ByteBuffer.wrap(m)));
        }, es);
        ms.registerHandler(MESSAGETYPE_CONFIRM, (a,m) -> {
            handleConfirm(a, Message.decodeValues(ByteBuffer.wrap(m)));
        }, es);
    }

    public CompletableFuture<Map<Long, byte[]>> makeTransaction(Map<Long, ValueOrServer> values) {
        TransactionGet.Transaction transaction;
        lock.lock();
        try {
            transaction = new Transaction(local_id, transactionId++, values, peers.keySet());
            transactions.put(transaction.id, transaction);
        } finally {
            lock.unlock();
        }

        Map<Integer, byte[]> messages = transaction.getMessages();
        if(messages.size() > 0)
            for(Map.Entry<Integer, byte[]> entry : transaction.getMessages().entrySet()) {
                System.out.println("[Sent][" + MESSAGETYPE_TRANSACTION + "|" + peers.get(entry.getKey()) + "]");
                ms.sendAsync(peers.get(entry.getKey()), MESSAGETYPE_TRANSACTION, entry.getValue());
            }
        else {
            transaction.completedTransaction.complete(transaction.acc);
            transactions.remove(transaction.id);
        }
        return transaction.completedTransaction;
    }

    void handleTransaction(Address address, Message message) {
        lock.lock();
        try {
            Set<Long> requestedKeys = message.keys;
            Map<Long, byte[]> values = kvp.get(requestedKeys).entrySet().stream()
                    .filter(entry -> entry.getValue().isValue() && requestedKeys.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
            byte[] buf = Message.encode(message.id, values);
            ms.sendAsync(address , MESSAGETYPE_CONFIRM , buf);
        } finally {
            System.out.println("[Received]" + "[" + MESSAGETYPE_TRANSACTION + "|" + address + "]: " + message);
            lock.unlock();
        }
    }

    void handleConfirm(Address address, Message message) {
        try {
            TransactionGet.Transaction transaction = transactions.get(message.id);
            transaction.addKeys(peers_rev.get(address), message.values);
            if (transaction.missingPeers.isEmpty()) {
                transaction.completedTransaction.complete(transaction.acc);
                transactions.remove(transaction.id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("[Received]" + "[" + MESSAGETYPE_CONFIRM + "|" + address + "]: " + message.id + " " + Arrays.toString(message.values.entrySet().toArray()));
            lock.unlock();
        }
    }

}
