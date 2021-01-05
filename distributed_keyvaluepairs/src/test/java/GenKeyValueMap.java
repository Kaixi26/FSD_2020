import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GenKeyValueMap {
    private static Random random = new Random();

    public static Map<Long, byte[]> gen(int length, long max){
        Map<Long, byte[]> ret = new HashMap<>();
        for(int i=0; i<length; i++){
            byte[] tmp = new byte[(Math.abs(random.nextInt()) % 10) + 1];
            random.nextBytes(tmp);
            ret.put(Math.abs(random.nextLong()) % max, tmp);
        }
        return ret;
    }
}
