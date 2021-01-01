package Client;

import spullara.nio.channels.FutureSocketChannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Stub {

    FutureSocketChannel server;
    boolean isActive = false;
    boolean isClosed = false;
    Lock lock = new ReentrantLock();
    ByteBuffer buf = ByteBuffer.allocate(1024);

    public Stub(SocketAddress address) throws IOException {
        server = new FutureSocketChannel();
        server.connect(address);
    }

    public void close(){
        lock.lock();
        try {
            server.close();
            isClosed = true;
        } finally {
            lock.unlock();
        }
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
        if(isClosed || !turnActive()) return null;

        CompletableFuture<Void> ret = new CompletableFuture<>();
        Message message = new Message(values);
        server.write(ByteBuffer.wrap(message.encode())).thenAcceptAsync(wr -> {
        });
        server.read(buf).thenAcceptAsync(rd -> {
            lock.lock();
            try {
                buf.clear();
                turnInactive();
                ret.complete((Void) null);
            } catch (Exception e){
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        });
        return ret;
    }

    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> keys){
        if(isClosed || !turnActive()) return null;

        CompletableFuture<Map<Long, byte[]>> ret = new CompletableFuture<>();
        Message message = new Message(keys);
        server.write(ByteBuffer.wrap(message.encode())).thenAcceptAsync(wr -> {
        });
        server.read(buf).thenAcceptAsync(rd -> {
            try {
                buf.flip();
                Message messageRecv = Message.decode(buf);
                buf.clear();
                turnInactive();
                if (messageRecv != null) ret.complete(messageRecv.getValues());
                else ret.complete(null);
            } finally {
                lock.unlock();
            }
        });
        return ret;
    }
}
