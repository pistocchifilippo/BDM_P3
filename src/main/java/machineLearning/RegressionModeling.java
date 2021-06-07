package machineLearning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.commons.io.FileUtils;

import java.io.IOException;

public class RegressionModeling implements RegressionModel{

    @Override
    public Map<String, Integer> train() throws IOException {

        SparkConf sparkConf = new SparkConf().setAppName("CART").setMaster("local[*]").set("spark.executor.memory","1g");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        // Data Source
        String datapath = "src/main/resources/p3_integrated.csv";
        // Position of the label in CSV columns (variable to be predicted)
        int position = 5;
        // Position of the variables not considered in the tree
        int[] disregard = {1,2,3};
        char fieldSep = ',';
        // Map to be used to relate a Neighborhood ID to the integer in the model
        Map<String, Integer> idMap = new HashMap<String, Integer>();
        // Converting the CSV to SVM in order to load it directly in JAVARDD for modeling
        String datapath_out = "src/main/resources/p3_integrated.svm";
        idMap = LibSVMConversion.convertRecords(datapath, datapath_out, position, fieldSep, disregard);
        // Creating a Random Forest Predictive Model
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), datapath_out).toJavaRDD();
        // Splitting in Training and Test Data set (80% and 20%)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.8, 0.2});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];
        // Considering the Neighborhood Id as a categorical feature
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        categoricalFeaturesInfo.put(0, idMap.size());
        // Number of trees
        int numTrees = 5;
        String featureSubsetStrategy = "auto";
        String impurity = "variance";
        int maxDepth = 4;
        int maxBins = idMap.size();
        int seed = 12345;
        // Train a RandomForest model.
        RandomForestModel model = RandomForest.trainRegressor(trainingData,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
        // Evaluate model on test instances and compute test error (calculated as the avg percentual error)
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        double testError = predictionAndLabel.mapToDouble(pl -> {
            double diff = (pl._1() - pl._2())/pl._2();
            return diff;
        }).mean();

        System.out.println("Test Average Error: " + testError);
        System.out.println("Learned regression forest model:\n" + model.toDebugString());

        // Save and load model


        try {
            FileUtils.deleteDirectory(new File("target/Model"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        model.save(jsc.sc(), "target/Model");
        RandomForestModel sameModel = RandomForestModel.load(jsc.sc(),"target/Model");

        jsc.stop();
        return idMap;
    }
}
