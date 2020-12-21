package Database;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KeyValue {

    Map<Long, byte[]> database = new HashMap<>();

    public KeyValue(){
    }

    public void put(Map<Long, byte[]> values){
        database.putAll(values);
    }

    public Map<Long, byte[]> get(Collection<Long> keys){
        Map<Long, byte[]> ret = new HashMap<>();
        for(Long key : keys)
            if(database.containsKey(key))
                ret.put(key, database.get(key));
        return ret;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder().append("[ ");
        for(Map.Entry<Long, byte[]> entry : database.entrySet())
            str.append(entry.getKey()).append("=").append(new String(entry.getValue())).append(" ");
        return str.append("]").toString();
    }
}
