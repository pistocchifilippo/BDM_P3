import ingestion.Kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import static java.util.Map.entry;

public class Main {

	private static final SparkConf conf = new SparkConf().setAppName("P3").setMaster("local[*]");
	private static final JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

	public static void main(String[] args) throws InterruptedException {

		// The key is not defined in our stream, ignore it
		JavaDStream<String> stream = Kafka.ingest(conf,ssc).map(t -> t.value());
		stream.print();

		ssc.start();
		ssc.awaitTermination();
	}
}

