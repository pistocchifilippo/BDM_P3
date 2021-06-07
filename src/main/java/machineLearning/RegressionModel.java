package machineLearning;

public interface RegressionModel {

    TrainedRegressionModel train();

    interface TrainedRegressionModel {
        long predict();
    }

}
