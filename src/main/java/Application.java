import stream.BloomFilterAnalysis;
import stream.HeavyHittersAnalysis;
import stream.ingestion.Kafka;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class Application {

	private static final SparkConf conf = new SparkConf().setAppName("P3").setMaster("local[*]");
	private static final JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

	public static void main(String[] args) throws InterruptedException {

		JavaDStream<String> stream = Kafka.ingest(conf,ssc).map(t -> t.value());

//		new HeavyHittersAnalysis().analyze(stream);
		new BloomFilterAnalysis(Arrays.asList("Q3297056","Q3389521","Q3753110")).analyze(stream);
		stream.print();

		ssc.start();
		ssc.awaitTermination();
	}
}

