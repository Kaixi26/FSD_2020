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
}
