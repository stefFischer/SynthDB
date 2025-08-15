package at.sfischer.synth.db;

import kotlin.Pair;

import java.util.LinkedHashMap;
import java.util.Map;

public class Utils {

    public static <K, V> Map<K, V> linkedMap(Pair<K, V>... keyValuePairs){
        Map<K, V> map = new LinkedHashMap<>();
        for (Pair<K, V> pair : keyValuePairs) {
            map.put(pair.getFirst(), pair.getSecond());
        }
        return map;
    }
}
