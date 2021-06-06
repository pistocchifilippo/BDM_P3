import machineLearning.RegressionModeling;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import stream.BloomFilterAnalysis;
import stream.HeavyHittersAnalysis;
import stream.ingestion.Kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Application {

	public static void main(String[] args) throws InterruptedException, IOException {

		Map<String, Integer> idMap = new HashMap<String, Integer>();
		RegressionModeling Regression = new RegressionModeling();
		idMap = Regression.train();

		final SparkConf conf = new SparkConf().setAppName("P3").setMaster("local[*]");
		final JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

		ssc.checkpoint("src/main/resources/checkpoint.txt");

		JavaDStream<String> stream = Kafka.ingest(conf,ssc).map(t -> t.value());

		switch(args[0]) {
			case "bloom_filter":
				new BloomFilterAnalysis(Arrays.asList("Q3297056","Q3389521","Q3753110")).analyze(stream);
				break;
			case "heavy_hitter":
				new HeavyHittersAnalysis().analyze(stream);
				break;
			case "prediction":
				System.out.println("TBC");
				break;
			default:
				System.out.println("This mode is not still supported :(");
		}

		 ssc.start();
		 ssc.awaitTermination();
	}
}

