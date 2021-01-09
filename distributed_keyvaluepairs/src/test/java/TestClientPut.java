import Client.Stub;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;

public class TestClientPut {
    static Random random = new Random();

    static int active = 0;
    final static Object sync = new Object();

    public static void main(String[] args) throws Exception {
        int[] ports = {10000, 10001, 10002};
        //for(int i=0; i<10; i++)
        checkWithN(50, 10000, ports);
        System.out.println("Passed all tests.");
    }

    static void checkWithN(int stubs, int maxSends, int[] ports) throws Exception {
        Stub[] stub = new Stub[stubs];
        for(int i=0; i<stub.length; i++) {
            stub[i] = new Stub(new InetSocketAddress("localhost", ports[Math.abs(random.nextInt()) % ports.length]));
            stub[i].start().get();
        }
        synchronized (sync) { active = stub.length;}
        int numPut = maxSends; // Math.abs(random.nextInt()) % maxSends;
        for(int i=0; i<stub.length; i++)
            sendN(i, stub[i], numPut);
        synchronized (sync) {
            while (active > 0) {
                sync.wait();
            }
        }
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
