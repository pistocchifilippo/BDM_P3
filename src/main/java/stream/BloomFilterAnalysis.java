package stream;

import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A bloom filter have been implemented in order to not allow the ingestion of particular data.
 * Let's suppose that the stream source is producing some neighbourhood that we don't want consider during the analysis.
 * The purpose of this Bloom Filter is exactly block that kind of data.
 */
public class BloomFilterAnalysis implements DataStreamAnalysis {

    private static final int M = 60;
    private static final List<Boolean> bitMap = new ArrayList<Boolean>(Collections.nCopies(M, false));

    public BloomFilterAnalysis(final List<String> block) {
        block.stream().map(e -> Math.abs(e.hashCode())%M).forEach(e -> bitMap.set(e, true));
    }

    @Override
    public void analyze(JavaDStream<String> stream) {
        stream
                .map(e -> e.split(",")[1])
                .mapToPair(e -> new Tuple2<>(e,Math.abs(e.hashCode())))
                .mapToPair(e -> new Tuple2<>(e._1, bitMap.get(e._2 % M) ? "BLOCKED" : "PASS"))
                .print();
    }

    public interface Hashing extends Function2<Integer, Integer, Integer> {};
    public interface Encoding extends Function1<String, Integer> {};

}
