package Client;

import Database.Database;
import spullara.nio.channels.FutureServerSocketChannel;
import spullara.nio.channels.FutureSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class Handler {

    private final int port;
    private final Distributed.Lock lock;
    private final Distributed.Transaction transaction;
    private final Database database;

    private FutureServerSocketChannel server;

    public Handler(int port, Distributed.Lock lock, Distributed.Transaction transaction, Database database) {
        this.port = port;
        this.lock = lock;
        this.transaction = transaction;
        this.database = database;
    }

    public Handler start() throws IOException {
        this.server = new FutureServerSocketChannel();
        this.server.bind(new InetSocketAddress(port));
        acceptConnections();
        return this;
    }

    private void acceptConnections(){
        server.accept().thenAcceptAsync(client -> {
            try {
                //System.out.println("[R][Client Connect]");
                ByteBuffer buf = ByteBuffer.allocate(10240);
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
                //System.out.println("[R][Client Disconnect]");
                return;
            }
            Message message = Message.decode(buf);
            //System.out.println("[R][Client(" + rd + "B)]: " + new String(buf.array()) + "(" + message + ")");
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
        lock.whenLocked().thenAcceptAsync(unlocker -> {
            transaction.put(values.keySet())
                    .thenAcceptAsync(_void -> {
                        database.putLocal(values);
                        client.write(ByteBuffer.wrap(ByteBuffer.allocate(1).put((byte) 1).array())).thenAcceptAsync(___v -> {
                            //System.out.println("[S][Client] " + 1);
                        });
                        unlocker.complete(null);
                    });
        });
    }

    private void handleGet(FutureSocketChannel client, Collection<Long> keys){
        lock.whenLocked().thenAcceptAsync(unlocker -> {
            transaction.get(database.getExternal(keys))
                    .thenAcceptAsync(map -> {
                        map.putAll(database.getLocal(keys));
                        Message message = new Message(map);
                        byte[] bytes = message.encode();
                        client.write(ByteBuffer.wrap(bytes)).thenAcceptAsync(wr -> {
                            //System.out.println("[S][Client] " + message + " " + Arrays.toString(bytes));
                        });
                        unlocker.complete(null);
                    });
        });
    }

}
