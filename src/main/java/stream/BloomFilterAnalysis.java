package stream;

import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Function1;
import scala.Int;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
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

}
