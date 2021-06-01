package stream;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

import java.text.DecimalFormat;

/**
 * This class is a specific implementation of DataStreamAnalysis, specialized in finding the Heavy Hitters
 *
 * The heavy hitter in this particular case will be calculate on the neighbourhood id, in order to find out
 * which neighbourhood is the most produced by the stream.
 */
public class HeavyHittersAnalysis implements DataStreamAnalysis {

    private static final int NEIGHBOURHOOD_ID = 1;
    private static final double THRESHOLD = 0.2;

    @Override
    public void analyze(JavaDStream<String> stream) {
        stream
                .mapToPair(s -> new Tuple2<>(s.split(",")[NEIGHBOURHOOD_ID],1))
                .window(new Duration(1000 * 60), new Duration(1000))
                .reduceByKey((a,b) -> a+b)
                .foreachRDD(a -> {
                    long size = a.count();
                    System.out.println("\n" + "RATIO:");
                    a.mapToPair(t -> new Tuple2<>(t._1, (double) t._2 / size))
                            .foreach(t -> System.out.println("[" + t._1 + ", " + new DecimalFormat("0.00").format(t._2) + "]" + ((t._2 >= THRESHOLD) ? " => HEAVY HITTER" : "")));
                });
    }
}
