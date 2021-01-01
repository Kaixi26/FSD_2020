import Client.Stub;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class FixedClient {

    final static Map<Long, byte[]> localMap = new HashMap<>();
    static Map<Long, byte[]> testMap0 = new HashMap<>();
    static Map<Long, byte[]> testMap1 = new HashMap<>();
    static Set<Long> testKeys = new HashSet<>();

    static {
        testMap0.put((long)0, "v0".getBytes());
        testMap0.put((long)1, "v1".getBytes());
        testMap1.put((long)0, "2v0".getBytes());
        testMap1.put((long)2, "v2".getBytes());
        testKeys.add((long)0);
        testKeys.add((long)1);
        testKeys.add((long)2);
    }

    static Random random = new Random();

    static Map<Long, byte[]> randomMap(){
        Map<Long, byte[]> ret = new HashMap<>();
        for(int i=0; i<5; i++)
            ret.put(Math.abs(random.nextLong()) % 10, (""+random.nextInt()).getBytes());
        return ret;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        AsynchronousChannelGroup.withFixedThreadPool(3, defaultThreadFactory());

        Client.Stub stub = new Client.Stub(new InetSocketAddress("localhost", 10000));
        Client.Stub stub2 = new Client.Stub(new InetSocketAddress("localhost", 10000));
        Client.Stub stub3 = new Client.Stub(new InetSocketAddress("localhost", 10002));

        //stub.put(testMap0).thenAcceptAsync(_v -> {
        //    stub2.put(testMap1).thenAcceptAsync(__v -> {
        //        stub.get(testKeys).thenAcceptAsync(values -> {
        //            for(Map.Entry<Long, byte[]> entry : values.entrySet())
        //                System.out.println(entry.getKey() + " " + new String(entry.getValue()));
        //        });
        //    });
        //});
        sendN(stub, 10);
        sendN(stub2, 3);
        sendN(stub3, 6);

        Thread.sleep(1000 * 3);
        System.out.println("send");
        stub.get(localMap.keySet()).thenAcceptAsync(map -> {
            try {
                System.out.println("recv");
                for (Map.Entry<Long, byte[]> entry : localMap.entrySet()) {
                    if(!Arrays.equals(entry.getValue(), map.get(entry.getKey())))
                        System.out.println("Invalid for " + entry.getKey() + " " + new String(entry.getValue()) + " " + new String(map.get(entry.getKey())));
                }
                System.out.println("end");
            } catch (Exception e){
                e.printStackTrace();
            }
        });

        Thread.sleep(1000);

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
        System.out.println("Sending " + stub + " " + n);
        if(n<=0) return;
        Map<Long, byte[]> tmp = randomMap();
        stub.put(tmp).thenAcceptAsync(_v -> {
            synchronized (localMap) {
                localMap.putAll(tmp);
            }
            sendN(stub, n-1);
        });
    }

}
