package Client;

import spullara.nio.channels.FutureServerSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Handler {

    public Handler() throws IOException {
        FutureServerSocketChannel server =
                new FutureServerSocketChannel();
        server.bind(new InetSocketAddress(12345));

    }
}
