package Clock;

import java.nio.ByteBuffer;
import java.util.Objects;

public class Scalar {
    final int bytes = Integer.BYTES + Long.BYTES;
    public final int id;
    long timer;

    Scalar(int id, long timer){
        this.id = id;
        this.timer = timer;
    }

    public byte[] encode(){
        ByteBuffer buffer = ByteBuffer.allocate(bytes);
        buffer.putInt(id);
        buffer.putLong(timer);
        return buffer.array();
    }

    public static Scalar decode(ByteBuffer buffer){
        return new Scalar(buffer.getInt(), buffer.getLong());
    }

    @Override
    public String toString() {
        return id + ":" + timer;
    }

    static public int compareWithIdPriority(Scalar c1, Scalar c2){
        int diff = Long.compare(c1.timer, c2.timer);
        if(diff == 0)
            return Integer.compare(c1.id, c2.id);
        else return diff;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Scalar scalar = (Scalar) o;
        return id == scalar.id &&
                timer == scalar.timer;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timer);
    }
}
