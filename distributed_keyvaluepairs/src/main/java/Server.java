import Client.Handler;
import Database.Database;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Server {
    private final int internal_port;
    private final int external_port;
    private final int id;
    private final Map<Integer, Address> peers;
    private final Database database = new Database();

    private Distributed.Lock lock;
    private Distributed.Transaction transaction;
    private Handler clientHandler;

    public Server(int external_port, int internal_port, int id, Map<Integer, Address> peers){
        this.internal_port = internal_port;
        this.external_port = external_port;
        this.id = id;
        this.peers = peers;
    }

    public Server start() throws IOException {

        NettyMessagingService ms =
                new NettyMessagingService("kvp_servers"
                        , Address.from(internal_port)
                        , new MessagingConfig());
        ms.start().join();

        lock = new Distributed.Lock(id, peers, ms, "", false)
                .start();
        transaction = new Distributed.Transaction(id, database, peers, ms, "", false)
                .start();
        clientHandler = new Handler(external_port, lock, transaction, database)
                .start();

        return this;
    }
}
