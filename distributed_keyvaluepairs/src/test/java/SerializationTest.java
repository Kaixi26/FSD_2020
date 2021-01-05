import Serialization.KeyValueMap;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class SerializationTest {

    static int nTests = 1000;
    static boolean success = true;

    public static void main(String[] args) {

        for (int i = 0; i < nTests; i++) {
            Map<Long, byte[]> tmp = GenKeyValueMap.gen(10, 100);
            Map<Long, byte[]> tmp2 = KeyValueMap.decode(ByteBuffer.wrap(KeyValueMap.encode(tmp)));
            boolean equiv = tmp.size() == tmp2.size()
                    && tmp.keySet().stream().allMatch(k -> Arrays.equals(tmp.get(k), tmp2.get(k)));
            success = success && equiv;
        }
        System.out.println(success ? "SUCCESS" : "ERROR");
    }
}
