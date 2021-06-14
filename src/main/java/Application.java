import machineLearning.RegressionModeling;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import stream.BloomFilterAnalysis;
import stream.HeavyHittersAnalysis;
import stream.ingestion.Kafka;
import java.util.Arrays;


public class Application {

	public static void main(String[] args) throws InterruptedException {

		final SparkConf conf = new SparkConf().setAppName("P3").setMaster("local[*]");
		
		switch(args[0]) {
			case "bloom_filter":

				final JavaStreamingContext ssc1 = new JavaStreamingContext(conf, new Duration(1000));
				JavaDStream<String> stream1 = Kafka.ingest(conf, ssc1).map(t -> t.value());
				new BloomFilterAnalysis(Arrays.asList("Q3297056", "Q3389521", "Q3753110")).analyze(stream1);
				ssc1.start();
				ssc1.awaitTermination();
				break;

			case "heavy_hitter":
				final JavaStreamingContext ssc2 = new JavaStreamingContext(conf, new Duration(1000));
				ssc2.checkpoint("src/main/resources/checkpoint");
				JavaDStream<String> stream2 = Kafka.ingest(conf, ssc2).map(t -> t.value());
				new HeavyHittersAnalysis().analyze(stream2);
				ssc2.start();
				ssc2.awaitTermination();
				break;

			case "prediction":
				new RegressionModeling().predict(conf);
				break;

			case "train":
				new RegressionModeling().train();
				break;

			default:
				System.out.println("This mode is not still supported :(");
		}
	}
}

