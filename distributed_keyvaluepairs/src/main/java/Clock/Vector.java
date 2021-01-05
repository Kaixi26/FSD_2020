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

    public String toString(){
        return localId + "@" + Arrays.toString(timers.entrySet().toArray());
    }


}
