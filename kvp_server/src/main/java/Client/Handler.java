package Client;

import Database.ValueOrServer;
import Lock.LockHandler;
import spullara.nio.channels.FutureServerSocketChannel;
import spullara.nio.channels.FutureSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class Handler {


    FutureServerSocketChannel server;
    LockHandler lockHandler;
    TransactionPut.Manager transactionPutManager;
    TransactionGet.Manager transactionGetManager;
    Database.KeyValue kvdb;

    public Handler(int port, Database.KeyValue kvdb, LockHandler lockHandler, TransactionPut.Manager transactionPutManager, TransactionGet.Manager transactionGetManager) throws IOException {
        this.lockHandler = lockHandler;
        this.transactionPutManager = transactionPutManager;
        this.transactionGetManager = transactionGetManager;
        this.kvdb = kvdb;
        server = new FutureServerSocketChannel();
        server.bind(new InetSocketAddress(port));
        acceptConnections();
    }

    private void acceptConnections(){
        server.accept().thenAcceptAsync(client -> {
            try {
                System.out.println("[Received][Client Connect]");
                ByteBuffer buf = ByteBuffer.allocate(1024);
                handleClient(client, buf);
                acceptConnections();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    //TODO: handle bigger payloads
    private void handleClient(FutureSocketChannel client, ByteBuffer buf) {
        buf.clear();
        client.read(buf).thenAcceptAsync(rd -> {
            buf.flip();
            if(rd <= 0) {
                System.out.println("[Received][Client Disconnect]");
                return;
            }
            Message message = Message.decode(buf);
            System.out.println("[Received][Client(" + rd + "B)]: " + new String(buf.array()) + "(" + message + ")");
            buf.clear();
            handleClient(client, buf);
            if(message != null) {
                if (message.getValues() != null)
                    handlePut(client, message.getValues());
                if (message.getKeys() != null)
                    handleGet(client, message.getKeys());
            }
        });
    }

    private void handlePut(FutureSocketChannel client, Map<Long, byte[]> values){
        lockHandler.lock().thenAcceptAsync((_v)-> {
            kvdb.put(values.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new ValueOrServer(entry.getValue()))));
            System.out.println("Lock acquired.");
            transactionPutManager.makeTransaction(values.keySet()).thenAcceptAsync((__v) -> {
                try {
                    System.out.println("Transaction finished.");
                    client.write(ByteBuffer.wrap(ByteBuffer.allocate(1).put((byte) 1).array())).thenAcceptAsync(___v -> {
                        System.out.println("[Sent][Client] " + 1);
                    });
                } finally {
                    lockHandler.unlock();
                }
            });
        });
    }

    private void handleGet(FutureSocketChannel client, Collection<Long> keys){
        lockHandler.lock().thenAcceptAsync((_v)-> {
            System.out.println("Lock acquired.");
            transactionGetManager.makeTransaction(kvdb.get(keys)).thenAcceptAsync(tmp -> {
                try {
                    Message message = new Client.Message(tmp);
                    byte[] bytes = message.encode();
                    client.write(ByteBuffer.wrap(bytes)).thenAcceptAsync(wr -> {
                        System.out.println("[Sent][Client] " + message + " " + Arrays.toString(bytes));
                    });
                } finally {
                    lockHandler.unlock();
                }
            });
        });
    }

}
