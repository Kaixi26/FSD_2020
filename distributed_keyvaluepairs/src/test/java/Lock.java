import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class Lock {
    final static Random random = new Random();
    final static int numServers = 10;
    final static int numAttempts = 100;
    static int testNum = numServers * numAttempts;

    public static void main(String[] args) throws Exception {
        NettyMessagingService[] ms = new NettyMessagingService[numServers];
        Distributed.Lock[] locks = new Distributed.Lock[numServers];
        Map<Integer, Address> allPeers = new HashMap<>();
        for(int i=0; i<numServers; i++) {
            allPeers.put(i, Address.from("localhost", 10000 + i));
            ms[i] = new NettyMessagingService("test"
                    , allPeers.get(i)
                    , new MessagingConfig());
            ms[i].start().join();
        }

        for(int i=0; i<numServers; i++){
            Map<Integer, Address> peers = new HashMap<>(allPeers);
            peers.remove(i);
            locks[i] = new Distributed.Lock(i, peers, ms[i], "", false).start();
        }
        for(int i=0; i<numServers; i++)
            attemptNLocks(i + "", locks[i], numAttempts);
        while(System.in.read() != 'q')
            System.out.println(testNum);
        System.exit(0);
    }

    private static void attemptNLocks(String label, Distributed.Lock lock, int attemptsLeft){
        if(attemptsLeft <= 0)
            System.out.println("Attempts over on " + label + ".");
        else {
//            if(attemptsLeft % 500 == 0)
            try {
                Thread.sleep(Math.abs(random.nextInt()) % 20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Attempts missing on " + label + " " + attemptsLeft + ".");
            lock.whenLocked().thenAcceptAsync(unlocker -> {
                testNum--;
                attemptNLocks(label, lock, attemptsLeft-1);
                unlocker.complete(null);
            });
        }
    }
}
