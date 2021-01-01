package Database;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KeyValue {

    Map<Long, ValueOrServer> database = new HashMap<>();

    public KeyValue(){
    }

    public void put(Map<Long, ValueOrServer> values){
        database.putAll(values);
    }

    public Map<Long, ValueOrServer> get(Collection<Long> keys){
        Map<Long, ValueOrServer> ret = new HashMap<>();
        for(Long key : keys)
            if(database.containsKey(key))
                ret.put(key, database.get(key));
        return ret;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder().append("[ ");
        for(Map.Entry<Long, ValueOrServer> entry : database.entrySet())
            str.append(entry.getKey()).append("=")
                    .append(entry.getValue().isValue() ? new String(entry.getValue().getValue()) : ("s" + entry.getValue().getServer()))
                    .append(" ");
        return str.append("]").toString();
    }
}
