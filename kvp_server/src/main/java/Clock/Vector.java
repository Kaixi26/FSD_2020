package Clock;

import java.nio.ByteBuffer;
import java.util.*;

public class Vector {

    private final int bytes;
    public final int localId;
    private final Map<Integer, Long> timers = new HashMap<>();

    public Vector(int localId, Collection<Integer> ids){
        this.localId = localId;
        this.timers.put(localId, (long) 0);
        for(int id : ids)
            this.timers.put(id, (long) 0);
        this.bytes =
                Integer.BYTES * 2
                        + timers.size() * (Integer.BYTES + Long.BYTES);
    }

    public Vector(int localId, Map<Integer, Long> timers){
        this.localId = localId;
        this.timers.putAll(timers);
        this.bytes =
                Integer.BYTES * 2
                + timers.size() * (Integer.BYTES + Long.BYTES);
    }

    public void increment(){
        this.timers.put(localId, this.timers.get(localId) + 1);
    }

    public Scalar getLocal(){
        return new Scalar(this.localId, timers.get(this.localId));
    }

    public void updateWith(Scalar clock){
        this.timers.put(clock.id, Math.max(this.timers.get(clock.id), clock.timer));
        this.timers.put(localId, Math.max(this.timers.get(localId) + 1, clock.timer + 1));
    }

    public boolean succeeds(Scalar clock){
        for(Long time : timers.values())
            if(time <= clock.timer)
                return false;
        return true;
    }


//    public byte[] encode(){
//        ByteBuffer buf = ByteBuffer.allocate(bytes);
//        buf.putInt(localId);
//        buf.putInt(timers.size());
//        for(Map.Entry<Integer, Long> entry : timers.entrySet()){
//            buf.putInt(entry.getKey());
//            buf.putLong(entry.getValue());
//        }
//        return buf.array();
//    }
//
//    public static Vector decode(ByteBuffer buffer){
//        int localId = buffer.getInt();
//        int size = buffer.getInt();
//        Map<Integer, Long> timers = new HashMap<>();
//        for(int i=0; i<size; i++)
//            timers.put(buffer.getInt(), buffer.getLong());
//        return new Vector(localId, timers);
//    }

//    public boolean happenedBefore(Vector clock){
//        boolean allSmallerOrEqual = true;
//        boolean anyBigger = false;
//        for(Map.Entry<Integer, Long> entry : this.timers.entrySet()) {
//            allSmallerOrEqual = allSmallerOrEqual
//                    && entry.getValue() <= clock.timers.get(entry.getKey());
//            anyBigger = anyBigger
//                    || entry.getValue() < clock.timers.get(entry.getKey());
//        }
//        return allSmallerOrEqual && anyBigger;
//    }
//
//    public boolean happenedAfter(Vector clock){
//        return clock.happenedBefore(this);
//    }


    public String toString(){
        return localId + "@" + Arrays.toString(timers.entrySet().toArray());
    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        Vector vector = (Vector) o;
//        return bytes == vector.bytes &&
//                localId == vector.localId &&
//                Objects.equals(timers, vector.timers);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(bytes, localId, timers);
//    }

}
