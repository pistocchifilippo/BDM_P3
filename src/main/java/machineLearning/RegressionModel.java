package machineLearning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public interface RegressionModel {

    TrainedRegressionModel train();

    TrainedRegressionModel predict(SparkConf conf) throws InterruptedException;

    interface TrainedRegressionModel {
    }

}
