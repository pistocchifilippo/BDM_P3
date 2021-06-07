package util;

import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class NeighbourhoodEncoding {

    public static void writeNeighbourhoodEncoding(final JavaSparkContext spark, final String filePath, final String outputPath) {
        AtomicInteger i = new AtomicInteger();

        spark.textFile(filePath)
                .map(e -> e.split(",")[0])
                .distinct()
                .repartition(1)
                .map(e -> e + "," + i.getAndIncrement())
                .saveAsTextFile(outputPath);

        spark.textFile("src/main/resources/neigh_encoding").foreach(e -> System.out.println(e));
    }

    public static Map<String,Integer> neighbourhoodEncoding(final JavaSparkContext spark, final String filePath) {
        return
                spark.textFile(filePath).mapToPair(s -> {
                    final String[] split = s.split(",");
                    return new Tuple2<>(split[0],Integer.valueOf(split[1]));
                }).collectAsMap();
    }
}
