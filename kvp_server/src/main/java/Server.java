import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Server {
    final int internal_port;
    final int external_port;
    final int id;
    final Map<Integer, Address> peers;
    LockHandler lockHandler;
    Transaction.Manager transactionManager;
    Database.KeyValue kvdb = new Database.KeyValue();

    public Server(int external_port, int internal_port, int id, Map<Integer, Address> peers){
        this.internal_port = internal_port;
        this.external_port = external_port;
        this.id = id;
        this.peers = peers;
    }

    public void start(){
        ScheduledExecutorService es =
                Executors.newScheduledThreadPool(1);

        NettyMessagingService ms =
                new NettyMessagingService("kvp_servers"
                        , Address.from("localhost", internal_port)
                        , new MessagingConfig());
        ms.start().join();

        lockHandler = new LockHandler(id, peers, ms, es);
        transactionManager = new Transaction.Manager(peers.values(), kvdb, ms, es);


        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String input = "";
        try {
            while (!input.matches("^:q ?.*")) {
                input = reader.readLine();
                switch (input) {
                    case ":put":
                        lockHandler.lock().thenAcceptAsync((_v)-> {
                           System.out.println("Lock acquired poggers.");
                           transactionManager.makeTransaction(new HashMap<>()).thenAcceptAsync((__v) -> {
                               System.out.println("Transaction finished.");
                               lockHandler.unlock();
                           });
                        });
                        break;
                    case ":print":
                        System.out.println(lockHandler);
                    default:
                        break;
                }
            }
        } catch (Exception e){}
    }
}