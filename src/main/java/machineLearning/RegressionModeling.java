package machineLearning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.commons.io.FileUtils;
import stream.ingestion.Kafka;
import util.NeighbourhoodEncoding;
import java.io.IOException;


public class RegressionModeling implements RegressionModel{


    @Override
    public TrainedRegressionModel train() {

        SparkConf sparkConf = new SparkConf().setAppName("CART").setMaster("local[*]").set("spark.executor.memory","1g");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        // Data Source
        String datapath = "src/main/resources/p3_integrated.csv";
        Map<String,Integer> myMap = new HashMap<>();
        String mappath = "src/main/resources/neighbourhood_encoding.csv";
        new NeighbourhoodEncoding();
        myMap = NeighbourhoodEncoding.neighbourhoodEncoding(jsc, mappath);

        Map<String, Integer> finalMyMap = myMap;

        JavaRDD<LabeledPoint> parsedData = jsc.textFile(datapath)
                .filter(t -> !t.contains("neigh_id"))
                .map(t -> new LabeledPoint(Double.parseDouble(t.split(",")[5]),
                                Vectors.dense(finalMyMap.get(t.split(",")[0]),
                                        Double.parseDouble(t.split(",")[4]))));

        JavaRDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[]{0.8, 0.2});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];
        // Considering the Neighborhood Id as a categorical feature
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        categoricalFeaturesInfo.put(0, finalMyMap.size());
        // Number of trees
        int numTrees = 5;
        String featureSubsetStrategy = "auto";
        String impurity = "variance";
        int maxDepth = 4;
        int maxBins = finalMyMap.size();
        int seed = 12345;
        // Train a RandomForest model.
        RandomForestModel model = RandomForest.trainRegressor(trainingData,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
        // Evaluate model on test instances and compute test error (calculated as the avg percentual error)
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));

        RegressionMetrics metrics = new RegressionMetrics(predictionAndLabel.rdd());
        double mse =  metrics.meanSquaredError();
        double r2 = metrics.r2();
        double rmse = metrics.rootMeanSquaredError();
        System.out.println("MSE = " + mse);
        System.out.println("R^2 = " + r2);
        System.out.println("RMSE = " + rmse);

        // Save and load model
        try {
            FileUtils.deleteDirectory(new File("target/Model"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        model.save(jsc.sc(), "target/Model");
        jsc.stop();
        return null;
    }

    @Override
    public TrainedRegressionModel predict(SparkConf conf) throws InterruptedException {

        JavaSparkContext jsc = new JavaSparkContext(conf);
        RandomForestModel sameModel = RandomForestModel.load(jsc.sc(),"target/Model");
        new NeighbourhoodEncoding();
        String mappath = "src/main/resources/neighbourhood_encoding.csv";
        Map<String,Integer> myMap = new HashMap<>();
        myMap = NeighbourhoodEncoding.neighbourhoodEncoding(jsc, mappath);
        jsc.stop();

        Map<String, Integer> finalMyMap = myMap;

        JavaStreamingContext ssc3 = new JavaStreamingContext(conf, new Duration(1000));
        JavaDStream<String> stream3 = Kafka.ingest(conf, ssc3).map(t -> t.value());
        stream3
                .map(t -> new Tuple2<>(t.split(",")[1], new LabeledPoint(1,
                        Vectors.dense(finalMyMap.get(t.split(",")[1]),
                                Double.parseDouble(t.split(",")[2])))))
                .map(p -> new Tuple2<>(p._1,sameModel.predict(p._2.features())))
                .print();
        ssc3.start();
        ssc3.awaitTermination();
        return null;
    }

}
