package Database;

import java.io.BufferedWriter;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

public class KeyValueLogger extends KeyValue {

    BufferedWriter logger;

    public KeyValueLogger(BufferedWriter logger){
        super();
        this.logger = logger;
    }

    public void put(Map<Long, byte[]> values) {
        super.put(values);
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        values.entrySet().stream().sorted(Comparator.comparingLong(Entry::getKey))
                .forEach(entry -> sb.append(entry.getKey()).append(":").append(new String(entry.getValue())).append(" "));
        sb.append("]");
        try {
            logger.write(sb.toString());
            logger.flush();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
