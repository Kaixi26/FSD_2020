package Client;

import Lock.LockHandler;
import spullara.nio.channels.FutureServerSocketChannel;
import spullara.nio.channels.FutureSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Handler {


    FutureServerSocketChannel server;
    LockHandler lockHandler;
    Transaction.Manager transactionManager;
    Database.KeyValue kvdb;

    public Handler(int port, Database.KeyValue kvdb, LockHandler lockHandler, Transaction.Manager transactionManager) throws IOException {
        this.lockHandler = lockHandler;
        this.transactionManager = transactionManager;
        this.kvdb = kvdb;
        server = new FutureServerSocketChannel();
        server.bind(new InetSocketAddress(port));
        acceptConnections();
    }

    private void acceptConnections(){
        server.accept().thenAcceptAsync(client -> {
            System.out.println("[Received][Client Connect]");
            ByteBuffer buf = ByteBuffer.allocate(1024);
            handleClient(client, buf);
            acceptConnections();
        });
    }

    //TODO: handle bigger payloads
    private void handleClient(FutureSocketChannel client, ByteBuffer buf) {
        buf.clear();
        client.read(buf).thenAcceptAsync(rd -> {
            buf.flip();
            if(rd <= 0) return;
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
            kvdb.put(values);
            System.out.println("Lock acquired.");
            transactionManager.makeTransaction(values).thenAcceptAsync((__v) -> {
                System.out.println("Transaction finished.");
                client.write(ByteBuffer.wrap(ByteBuffer.allocate(1).put((byte)1).array()));
                lockHandler.unlock();
            });
        });
    }

    private void handleGet(FutureSocketChannel client, Collection<Long> keys){
        lockHandler.lock().thenAcceptAsync((_v)-> {
            System.out.println("Lock acquired.");
            Message message = new Client.Message(kvdb.get(keys));
            byte[] bytes = message.encode();
            client.write(ByteBuffer.wrap(bytes)).thenAcceptAsync(wr -> {
                System.out.println("[Sent][Client] " + message + " " + Arrays.toString(bytes));
            });
            lockHandler.unlock();
        });
    }

}
