package Database;

public class ValueOrServer {
    final byte[] value;
    final Integer server;

    public ValueOrServer(byte[] value){
        this.value = value;
        this.server = null;
    }

    public ValueOrServer(int server){
        this.value = null;
        this.server = server;
    }

    public boolean isValue(){
        return value != null;
    }

    public boolean isServer(){
        return server != null;
    }

    public byte[] getValue() {
        return value;
    }

    public Integer getServer() {
        return server;
    }
}
