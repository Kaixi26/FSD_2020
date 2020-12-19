import io.atomix.utils.net.Address;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class Main {

    private static Map<Integer, Address> peersFromArgs(String[] args){
        Map<Integer, Address> peers = new HashMap<>();
        for(int i=3; i<args.length; i+=2) {
            String[] ip_port = args[i].split(":");
            int id = Integer.parseInt(args[i+1]);
            peers.put(id, Address.from(ip_port[0], Integer.parseInt(ip_port[1])));
        }
        return peers;
    }

    //args: [EXTERNAL_PORT] [INTERNAL_PORT] [LOCAL_ID] [IP:PORT ID]...
    //ie: 10000 10010 0 localhost:10011 1
    public static void main(String[] args) {
        int external_port = Integer.parseInt(args[0]);
        int internal_port = Integer.parseInt(args[1]);
        int id = Integer.parseInt(args[2]);
        Map<Integer, Address> peers = peersFromArgs(args);

        System.out.println("External Port: " + external_port + ", id: " + id);
        System.out.println("Internal Port: " + internal_port + ", id: " + id);
        System.out.println("Peers: " + peers.entrySet().toString());

        Server server = new Server(external_port, internal_port, id, peers);
        server.start();
    }
}
