import Client.Stub;
import jdk.nashorn.internal.runtime.ECMAException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class TestClientGet {
    static Random random = new Random();

    static int active = 0;
    final static Object sync = new Object();

    public static void main(String[] args) throws Exception {
        int[] ports = {10000, 10001, 10002};
        for(int i=0; i<1000; i++)
            checkWithN(20, 100, ports);
        System.out.println("Passed all tests.");
    }

    static void checkWithN(int stubs, int maxSends, int[] ports) throws IOException, InterruptedException, ExecutionException {
        Stub[] stub = new Stub[stubs];
        for(int i=0; i<stub.length; i++)
            stub[i] = new Stub(new InetSocketAddress("localhost", ports[Math.abs(random.nextInt()) % ports.length]));
        synchronized (sync) { active = stub.length;}
        int numPut = maxSends; // Math.abs(random.nextInt()) % maxSends;
        for(int i=0; i<stub.length; i++)
            sendN(i, stub[i], numPut);
        synchronized (sync) {
            while (active > 0) {
                sync.wait();
            }
        }
        Set<Long> keys = GenKeyValueMap.gen(10, 20).keySet();
        List<CompletableFuture<Map<Long, byte[]>>> maps = new ArrayList<>();
        for(int i=0; i<stub.length; i++)
            maps.add(i, stub[i].get(keys));
        for(int i=1; i<stub.length; i++)
            for(Long key : keys) {
                byte[] v1 = maps.get(i - 1).get().getOrDefault(key, new byte[0]);
                byte[] v2 = maps.get(i).get().getOrDefault(key, new byte[0]);
                if(!Arrays.equals(v1, v2)) {
                    System.out.println("FAIL");
                    System.exit(1);
                }
            }
        for(int i=0; i<stub.length; i++)
            stub[i].close();
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
        Map<Long, byte[]> tmp = GenKeyValueMap.gen(5, 10);
        //System.out.println("[Sending in " + tag + "] " + n + " left");
        stub.put(tmp).thenAcceptAsync(_v -> {
            //System.out.println("[Recv in " + tag + "] " + n + " left");
            sendN(tag, stub, n-1);
        });
    }
}
