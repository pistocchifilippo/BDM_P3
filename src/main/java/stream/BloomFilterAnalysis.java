package stream;

import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Function1;
import scala.Function2;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A bloom filter have been implemented in order to allow the ingestion of just some particular data.
 * Let's suppose that the stream source is producing some neighbourhood and we want consider just a few of them during the analysis.
 * The purpose of this Bloom Filter is exactly allow the ingestion of just some data.
 */
public class BloomFilterAnalysis implements DataStreamAnalysis {

    private static final int M = 60;
    private static final List<Boolean> bitMap = new ArrayList<>(Collections.nCopies(M, false));

    private final Encoding encoding = e -> Math.abs(e.hashCode());
    private final Hashing hashing = (value, module) -> value % module;

    public BloomFilterAnalysis(final List<String> pass) {
        pass.stream().map(e -> hashing.apply(encoding.apply(e),M)).forEach(e -> bitMap.set(e, true));
    }

    @Override
    public void analyze(JavaDStream<String> stream) {
        stream
                .map(e -> e.split(",")[1])
                .mapToPair(e -> new Tuple2<>(e,encoding.apply(e)))
                .mapToPair(e -> new Tuple2<>(e._1, bitMap.get(hashing.apply(e._2,M)) ? "PASS" : "BLOCKED"))
                .print();
    }

    public interface Hashing extends Function2<Integer, Integer, Integer>, Serializable {}
    public interface Encoding extends Function1<String, Integer>, Serializable {}

}
