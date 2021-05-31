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

	private static Map<String, Object> kafkaParams = Map.ofEntries(
			entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandshrew.fib.upc.edu:9092"),
			entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class),
			entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class),
			entry(ConsumerConfig.GROUP_ID_CONFIG, "gid")
	);
	private static Collection<String> topics = Arrays.asList("bdm_p3");

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("P3").setMaster("local[*]");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
		Logger.getRootLogger().setLevel(Level.ERROR);

		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
				ssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topics, kafkaParams)
		);

		// The key is not defined in our stream, ignore it
		JavaDStream<String> stream = kafkaStream.map(t -> t.value());
		stream.print();

		ssc.start();
		ssc.awaitTermination();
	}
}

