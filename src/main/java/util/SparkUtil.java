package util;

import org.apache.spark.SparkConf;

public class SparkUtil {

    public static SparkConf createSparkConf() {
        return new SparkConf().setAppName("P3").setMaster("local[*]");
    }

}
