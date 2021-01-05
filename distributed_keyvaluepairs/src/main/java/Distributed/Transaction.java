package Distributed;

import Database.Database;
import Serialization.KeySet;
import Serialization.KeyValueMap;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import javax.naming.directory.AttributeInUseException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Transaction {
    private final String MESSAGETYPE_ACKNOWLEAGEMENT;
    private final String MESSAGETYPE_TRANSACTION_GET;
    private final String MESSAGETYPE_TRANSACTION_PUT;
    private final Map<Integer, Address> peers;
    private final NettyMessagingService ms;
    private final ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
    private final Database database;
    private final int id;
    private final boolean debug;

    private byte state = 0; // 0, 1, 2 == Not active, Put, Get
    private int missingAcknowledgements = 0;
    private Map<Long, byte[]> valueAccumulator = null;
    private CompletableFuture<Void> putFuture;
    private CompletableFuture<Map<Long, byte[]>> getFuture;

    public Transaction(int id, Database database, Map<Integer, Address> peers, NettyMessagingService ms, String messagePrefix, boolean debug){
        this.peers = peers;
        this.ms = ms;
        this.database = database;
        this.debug = debug;
        this.id = id;
        this.MESSAGETYPE_ACKNOWLEAGEMENT = messagePrefix + "TRANS_CONFIRM";
        this.MESSAGETYPE_TRANSACTION_GET = messagePrefix + "TRANS_GET";
        this.MESSAGETYPE_TRANSACTION_PUT = messagePrefix + "TRANS_PUT";
    }

    public Transaction start(){
        ms.registerHandler(MESSAGETYPE_ACKNOWLEAGEMENT, this::handleAcknowledgement, es);
        ms.registerHandler(MESSAGETYPE_TRANSACTION_GET, this::handleGet, es);
        ms.registerHandler(MESSAGETYPE_TRANSACTION_PUT, this::handlePut, es);
        return this;
    }

    private void log(String log) {
        if(debug)
            System.out.println(log);
    }

    private void send(Address address, String type, byte[] buf, String log) {
        ms.sendAsync(address, type, buf).thenAccept(_void ->
                log("[S][" + type + "|" + address + "]: " + log));
    }

    private void handleAcknowledgement(Address address, byte[] message){
        try {
            missingAcknowledgements--;
            log("[R][" + MESSAGETYPE_ACKNOWLEAGEMENT + "|" + address + "]: " + missingAcknowledgements);
            if (state == 2)
                valueAccumulator.putAll(KeyValueMap.decode(ByteBuffer.wrap(message)));
            if (missingAcknowledgements <= 0) {
                if(state == 1) putFuture.complete(null);
                else if(state == 2) getFuture.complete(valueAccumulator);
                state = 0;
            }
        }catch (Exception e){e.printStackTrace();}
    }

    private void handlePut(Address address, byte[] message){
        log("[R][" + MESSAGETYPE_TRANSACTION_PUT + "|" + address + "]: ");
        ByteBuffer buf = ByteBuffer.wrap(message);
        int externalId = buf.getInt();
        database.putExternal(KeySet.decode(buf), externalId);
        send(address, MESSAGETYPE_ACKNOWLEAGEMENT, new byte[1], "");
    }

    private void handleGet(Address address, byte[] message){
        Set<Long> keys = KeySet.decode(ByteBuffer.wrap(message));
        log("[R][" + MESSAGETYPE_TRANSACTION_PUT + "|" + address + "]: " + keys);
        Map<Long, byte[]> values = database.getLocal(keys);
        send(address, MESSAGETYPE_ACKNOWLEAGEMENT, KeyValueMap.encode(values), values.toString());
    }

    public CompletableFuture<Void> put(Set<Long> keys){
        CompletableFuture<Void> ret = new CompletableFuture<>();
        es.execute(() -> {
            if(state != 0) {
                ret.completeExceptionally(new AttributeInUseException());
                System.out.println(ret);
                return;
            }
            state = 1;
            putFuture = ret;
            missingAcknowledgements = peers.size();
            valueAccumulator = null;
            byte[] encKeys = KeySet.encode(keys);
            byte[] m = ByteBuffer.allocate(Integer.BYTES + encKeys.length)
                    .putInt(id)
                    .put(encKeys).array();
            peers.values()
                    .forEach(address -> send(address, MESSAGETYPE_TRANSACTION_PUT, m,keys.toString()));
        });
        return ret;
    }

    public CompletableFuture<Map<Long, byte[]>> get(Map<Integer, Set<Long>> keys){
        CompletableFuture<Map<Long, byte[]>> ret = new CompletableFuture<>();
        es.execute(() -> {
            if(keys.size() <= 0) {
                ret.complete(new HashMap<>());
                return;
            }
            if(state != 0) {
                ret.completeExceptionally(new AttributeInUseException());
                return;
            }
            state = 2;
            getFuture = ret;
            missingAcknowledgements = keys.size();
            valueAccumulator = new HashMap<>();
            keys.forEach((key, value) -> {
                byte[] m = KeySet.encode(value);
                send(peers.get(key), MESSAGETYPE_TRANSACTION_GET, m, value.toString());
            });
        });
        return ret;
    }

}
