import Client.Stub;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TestClient {
    final static Map<Long, byte[]> localMap = new HashMap<>();
    static Random random = new Random();

    static int active = 0;
    final static Object sync = new Object();

    public static void main(String[] args) throws IOException, InterruptedException {
        int[] ports = {10000, 10001, 10002};
        //for(int i=0; i<10; i++)
        checkWithN(2, 10, ports);
        System.out.println("Passed all tests.");
    }

    static void checkWithN(int stubs, int maxSends, int[] ports) throws IOException, InterruptedException {
        Stub[] stub = new Stub[stubs];
        for(int i=0; i<stub.length; i++)
            stub[i] = new Stub(new InetSocketAddress("localhost", ports[i])); //ports[Math.abs(random.nextInt()) % ports.length]));
        synchronized (sync) { active = stub.length;}
        int numPut = maxSends; // Math.abs(random.nextInt()) % maxSends;
        for(int i=0; i<stub.length; i++)
            sendN(i, stub[i], numPut);
        synchronized (sync) {
            while (active > 0) {
                System.out.print(active + "... ");
                sync.wait();
            }
        }

        synchronized (sync) { active = 1; }
        checkGet(stub[Math.abs(random.nextInt()) % stub.length]);
        synchronized (sync) {
            while (active > 0) sync.wait();
        }
    }

    static Map<Long, byte[]> randomMap(){
        Map<Long, byte[]> ret = new HashMap<>();
        int size = Math.abs(random.nextInt()) % 5;
        for(int i=0; i<size; i++)
            ret.put(Math.abs(random.nextLong()) % 25, (""+random.nextInt() % 10).getBytes());
        return ret;
    }

    static void checkGet(Stub stub){
        stub.get(localMap.keySet()).thenAcceptAsync(map -> {
            for (Map.Entry<Long, byte[]> entry : localMap.entrySet()) {
                if(!Arrays.equals(entry.getValue(), map.get(entry.getKey()))) {
                    System.out.println("Invalid for " + entry.getKey() + " " + new String(entry.getValue()) + " " + new String(map.get(entry.getKey())));
                    System.exit(1);
                }
            }
            System.out.println("Passed test.");
            synchronized (sync) {
                active--;
                sync.notifyAll();
            }
        });

    }

    static void sendN(int tag, Stub stub, int n){
        System.out.println(tag + "-> " + n + "left");
        if(n<=0) {
            synchronized (sync) {
                active--;
                sync.notifyAll();
            }
            return;
        }
        Map<Long, byte[]> tmp = randomMap();
        //System.out.println("[Sending in " + tag + "] " + n + " left");
        stub.put(tmp).thenAcceptAsync(_v -> {
            //System.out.println("[Recv in " + tag + "] " + n + " left");
            synchronized (localMap) {
                localMap.putAll(tmp);
            }
            sendN(tag, stub, n-1);
        });
    }

}
