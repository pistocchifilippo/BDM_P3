package stream.ingestion;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static java.util.Map.entry;

public class Kafka {

    private static final Map<String, Object> kafkaParams = Map.ofEntries(
            entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandshrew.fib.upc.edu:9092"),
            entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class),
            entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class),
            entry(ConsumerConfig.GROUP_ID_CONFIG, "gid")
    );
    private static final Collection<String> topics = Arrays.asList("bdm_p3");

    public static JavaInputDStream<ConsumerRecord<String, String>> ingest(final SparkConf conf, final JavaStreamingContext ssc) {
        Logger.getRootLogger().setLevel(Level.ERROR);

        return KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );
    }
}
