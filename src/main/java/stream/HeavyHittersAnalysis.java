package stream;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is a specific implementation of DataStreamAnalysis, specialized in finding the Heavy Hitters
 *
 * The heavy hitter in this particular case will be calculate on the neighbourhood id, in order to find out
 * which neighbourhood is the most produced by the stream.
 */
public class HeavyHittersAnalysis implements DataStreamAnalysis {

    private static final int NEIGHBOURHOOD_ID = 1;
    private static final double THRESHOLD = 0.2;
    private static final int LIMIT = 5;

    private static Map<String,Integer> heavyHitters = new ConcurrentHashMap<>();

    @Override
    public void analyze(JavaDStream<String> stream) {

        Function3<String, Optional<Integer>, State<Map<String,Integer>>, Object> mappingFunction =
                (key,value,state) -> {
                    if (!state.exists()) {
                        state.update(heavyHitters);
                    }
                    int s = heavyHitters.getOrDefault(key,0);
                    heavyHitters.put(key, s + 1);
                    if (overflows(heavyHitters, LIMIT)) {
                        fixStructure();
                    }
                    return null;
                };

        stream
                .mapToPair(e -> new Tuple2<>(e.split(",")[NEIGHBOURHOOD_ID],1))
                .mapWithState(StateSpec.function(mappingFunction))
                .foreachRDD(e -> e.foreach(x -> {
                    System.out.println();
                    System.out.println(heavyHitters);
                }));
    }

    private boolean overflows(final Map<String,Integer> structure, final int limit) {
        return structure.size() > limit;
    }

    private void fixStructure() {
        final Map<String,Integer> m = new HashMap<>();

        heavyHitters.forEach((k, v) -> {
            if (v > 1) {
                m.put(k,v - 1);
            }
        });

        heavyHitters = m;
    }
}