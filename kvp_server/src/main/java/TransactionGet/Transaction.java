package TransactionGet;

import Database.ValueOrServer;
import io.atomix.utils.net.Address;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Transaction {
    final int local_id;
    final long id;
    Set<Integer> missingPeers;
    Map<Integer, Set<Long>> messages = new HashMap<>();
    Map<Long, byte[]> acc = new HashMap<>();
    CompletableFuture<Map<Long, byte[]>> completedTransaction = new CompletableFuture<>();

    Transaction(int local_id, long id, Map<Long, ValueOrServer> kvp, Set<Integer> missingPeers){
        this.id = id;
        this.local_id = local_id;
        this.missingPeers = kvp.values().stream().filter(ValueOrServer::isServer)
                .map(ValueOrServer::getServer).collect(Collectors.toSet());
        for(Map.Entry<Long, ValueOrServer> entry : kvp.entrySet()){
            if(entry.getValue().isValue())
                acc.put(entry.getKey(), entry.getValue().getValue());
            else {
                Set<Long> tmp = messages.getOrDefault(entry.getValue().getServer(), new HashSet<>());
                tmp.add(entry.getKey());
                messages.put(entry.getValue().getServer(), tmp);
            }
        }
    }

    Map<Integer, byte[]> getMessages(){
        return messages.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Message.encode(id, entry.getValue())));
    }

    void addKeys(int peer, Map<Long, byte[]> values){
        missingPeers.remove(peer);
        acc.putAll(values);

    }

}
