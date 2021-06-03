package stream;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import org.apache.spark.api.java.Optional;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is a specific implementation of DataStreamAnalysis, specialized in finding the Heavy Hitters
 *
 * The heavy hitter in this particular case will be calculate on the neighbourhood id, in order to find out
 * which neighbourhood is the most produced by the stream.
 */
public class HeavyHittersAnalysis implements DataStreamAnalysis {

    private static final int NEIGHBOURHOOD_ID = 1;
    private static final double THRESHOLD = 0.12;
    private static final Duration ONE_SECOND = new Duration(1000);
    private static final Duration ONE_MINUTE = new Duration(1000*60);

    @Override
    public void analyze(JavaDStream<String> stream) {

        JavaPairDStream<String,Integer> s = stream
                .mapToPair(e -> new Tuple2<>(e.split(",")[NEIGHBOURHOOD_ID],1))
                .window(ONE_MINUTE, ONE_SECOND)
                .reduceByKey((a,b) -> a + b)
                .persist();

        final AtomicInteger count = new AtomicInteger();
        s.foreachRDD(e -> count.set(e.values().reduce((a, b) -> a + b)));

        s.mapToPair(e -> new Tuple2<>(e._1,(double)e._2/count.get()))
                .foreachRDD(e -> {
                    System.out.println("\n" + "RATIO:");
                    e.foreach(n -> System.out.println(
                            "[" + n._1 + "," + String.format("%.2f", n._2) + "] " + ((n._2 >= THRESHOLD) ? "=> HEAVY HITTER" : ""
                            )));
                });

    }
}