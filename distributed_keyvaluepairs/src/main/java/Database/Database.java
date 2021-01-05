package Database;

import java.util.*;
import java.util.stream.Collectors;

public class Database {
    Map<Long, byte[]> localValues = new HashMap<>();
    Map<Long, Integer> externalValues = new HashMap<>();

    public Database(){}

    public void putLocal(Map<Long, byte[]> values){
        localValues.putAll(values);
        values.keySet()
                .forEach(key -> externalValues.remove(key));
    }

    public void putExternal(Set<Long> keys, int id){
        keys.forEach(key -> {
            localValues.remove(key);
            externalValues.put(key, id);
        });
    }

    public Map<Long, byte[]> getLocal(Collection<Long> keys){
        return keys.stream()
                .filter(key -> localValues.containsKey(key))
                .collect(Collectors.toMap(key -> key, key -> localValues.get(key)));
    }

    public Map<Integer, Set<Long>> getExternal(Collection<Long> keys){
        Map<Integer, Set<Long>> ret = new HashMap<>();
        keys.stream().filter(key -> externalValues.containsKey(key))
                .forEach(key -> {
                    int id = externalValues.get(key);
                    if(!ret.containsKey(id))
                        ret.put(id, new HashSet<>());
                    ret.get(id).add(key);
                });
        return ret;
    }

    public String toString() {
        return "db{" +
                "local=" + Arrays.toString(localValues.entrySet().stream().map(e -> e.getKey() + "=" + new String(e.getValue())).toArray()) +
                ", external=" + externalValues +
                '}';
    }
}
