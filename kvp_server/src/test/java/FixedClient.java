import Client.Stub;
import Transaction.Transaction;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.*;

import Transaction.TransactionMap;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class FixedClient {

    static Random random = new Random();

    static Map<Long, byte[]> randomMap(){
        Map<Long, byte[]> ret = new HashMap<>();
        for(int i=0; i<5; i++)
            ret.put(Math.abs(random.nextLong()) % 10, (""+i).getBytes());
        return ret;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        Client.Stub stub = new Client.Stub(new InetSocketAddress("localhost", 10000));
        Client.Stub stub2 = new Client.Stub(new InetSocketAddress("localhost", 10001));
        Client.Stub stub3 = new Client.Stub(new InetSocketAddress("localhost", 10002));

        sendN(stub, 10);
        sendN(stub2, 10);
        sendN(stub3, 10);

        Thread.sleep(1000 * 10);
    }

    /*
    public static void main(String[] args) throws IOException, InterruptedException {
        Client.Stub stub = new Client.Stub(new InetSocketAddress("localhost", 10000));
        Client.Stub stub2 = new Client.Stub(new InetSocketAddress("localhost", 10001));

        sendN(stub, 100);
        sendN(stub2, 100);
        //sendN(stub2, 10);
        //sendN(stub2, 100);
//        stub2.put(randomMap()).thenAccept(__v -> {
//            stub.put(randomMap()).thenAccept(_v -> {
//                System.out.println("Accepted.");
//            });
//        });

        Thread.sleep(10000);
    }
    */

    static void sendN(Stub stub, int n){
        if(n<=0) return;
        stub.put(randomMap()).thenAcceptAsync(_v -> {
            sendN(stub, n-1);
        });
    }

}
