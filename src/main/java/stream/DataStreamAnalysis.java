package stream;

import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

public interface DataStreamAnalysis extends Serializable {
    void analyze(JavaDStream<String> stream);
}
