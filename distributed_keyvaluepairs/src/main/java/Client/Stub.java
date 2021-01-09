package Client;

import spullara.nio.channels.FutureSocketChannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Stub {

    final SocketAddress address;
    FutureSocketChannel server;
    boolean isActive = false;
    Boolean isClosed = null;
    ByteBuffer buf = ByteBuffer.allocate(1024);

    public Stub(SocketAddress address) throws IOException {
        server = new FutureSocketChannel();
        this.address = address;
    }

    public CompletableFuture<Void> start(){
        synchronized (this){
            if(isClosed == null) {
                isClosed = false;
                return server.connect(address);
            }
            return null;
        }
    }

    public void close()  {
        synchronized (this){
            server.close();
            isClosed = true;
        }
    }

    boolean turnActive(){
        synchronized (this) {
            if (isClosed == null || isClosed || isActive) return false;
            isActive = true;
            return true;
        }
    }

    public CompletableFuture<Void> put(Map<Long, byte[]> values){
        if(!turnActive()) return null;

        CompletableFuture<Void> ret = new CompletableFuture<>();
        Message message = new Message(values);
        server.write(ByteBuffer.wrap(message.encode())).thenAcceptAsync(wr -> {
        });
        server.read(buf).thenAcceptAsync(rd -> {
            synchronized (this){
                buf.clear();
                isActive = false;
                ret.complete((Void) null);
            }
        });
        return ret;
    }

    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> keys){
        if(!turnActive()) return null;

        CompletableFuture<Map<Long, byte[]>> ret = new CompletableFuture<>();
        Message message = new Message(keys);
        server.write(ByteBuffer.wrap(message.encode())).thenAcceptAsync(wr -> {
        });
        server.read(buf).thenAcceptAsync(rd -> {
            synchronized (this){
                buf.flip();
                Message messageRecv = Message.decode(buf);
                buf.clear();
                isActive = false;
                if (messageRecv != null) ret.complete(messageRecv.getValues());
                else ret.complete(null);
            }
        });
        return ret;
    }
}
