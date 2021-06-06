package machineLearning;

import java.io.IOException;
import java.util.Map;

public interface RegressionModel {

    default Map<String, Integer> train() throws IOException {
        return null;
    }

    interface TrainedRegressionModel {
        long predict();
    }

}
