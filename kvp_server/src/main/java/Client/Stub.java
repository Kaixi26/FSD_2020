package Client;

import spullara.nio.channels.FutureSocketChannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Stub {

    FutureSocketChannel server;
    boolean isActive = false;
    Lock lock = new ReentrantLock();
    ByteBuffer buf = ByteBuffer.allocate(1024);

    public Stub(SocketAddress address) throws IOException {
        server = new FutureSocketChannel();
        server.connect(address);
    }

    boolean turnActive(){
        lock.lock();
        try {
            if (isActive) return false;
            isActive = true;
            return true;
        } finally {
            lock.unlock();
        }
    }

    void turnInactive(){
        lock.lock();
        isActive = false;
        lock.unlock();
    }

    public CompletableFuture<Void> put(Map<Long, byte[]> values){
        if(!turnActive()) return null;

        CompletableFuture<Void> ret = new CompletableFuture<>();
        Message message = new Message(values);
        server.write(ByteBuffer.wrap(message.encode())).thenAcceptAsync(wr -> {
        });
        server.read(buf).thenAcceptAsync(rd -> {
            buf.clear();
            turnInactive();
            ret.complete((Void) null);
        });
        return ret;
    }

    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> keys){
        if(!turnActive()) return null;

        CompletableFuture<Map<Long, byte[]>> ret = new CompletableFuture<>();
        Message message = new Message(keys);
        server.write(ByteBuffer.wrap(message.encode())).thenAcceptAsync(wr -> {
            System.out.println("Sent");
        });
        server.read(buf).thenAcceptAsync(rd -> {
            buf.flip();
            Message messageRecv = Message.decode(buf);
            buf.clear();
            turnInactive();
            if(messageRecv != null) ret.complete(messageRecv.getValues());
            else ret.complete(null);
        });
        return ret;
    }
}
