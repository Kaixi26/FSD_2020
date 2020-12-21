import Transaction.Transaction;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import Transaction.TransactionMap;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class FixedClient {
    public static void main(String[] args) throws IOException, InterruptedException {
        Map<Long, byte[]> testMap = new HashMap<>();
        testMap.put((long) 0, "0".getBytes());
        testMap.put((long) 1, "2".getBytes());
        testMap.put((long) 2, "4".getBytes());
        Map<Long, byte[]> testMap2 = new HashMap<>();
        testMap2.put((long) 0, "1".getBytes());
        testMap2.put((long) 3, "6".getBytes());
        testMap2.put((long) 4, "8".getBytes());
        Long[] col = {(long)0, (long)1, (long)2, (long) 3, (long) 4};

//        System.out.println(Arrays.toString(TransactionMap.encode(testMap)));
//        System.out.println(Arrays.toString(TransactionMap.decode(ByteBuffer.wrap(TransactionMap.encode(testMap)))
//                .entrySet().toArray()));

        Client.Stub stub = new Client.Stub(new InetSocketAddress("localhost", 10000));
        Client.Stub stub2 = new Client.Stub(new InetSocketAddress("localhost", 10001));
        stub2.put(testMap).thenAccept(__v -> {
            stub.put(testMap2).thenAccept(_v -> {
                System.out.println("Accepted.");
                stub.get(Arrays.asList(col)).thenAcceptAsync(values -> {
                    StringBuilder str = new StringBuilder().append("[ ");
                    for (Map.Entry<Long, byte[]> entry : values.entrySet())
                        str.append(entry.getKey()).append("=").append(new String(entry.getValue())).append(" ");
                    System.out.println(str.append("]").toString());
                });
            });
        });

        Thread.sleep(10000);
    }
}
