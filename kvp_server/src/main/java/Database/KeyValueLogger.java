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

    public void put(Map<Long, ValueOrServer> values) {
        super.put(values);
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("[ ");
            values.entrySet().stream().sorted(Comparator.comparingLong(Entry::getKey))
                    .forEach(e -> sb.append(e.getKey()));
            sb.append("]");
            logger.write(sb.toString());
            logger.flush();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
