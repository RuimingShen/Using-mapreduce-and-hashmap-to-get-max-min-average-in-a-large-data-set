import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducerclass extends Reducer<Text, Text, Text, Text> {


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;
        double sum = 0;
        int count = 0;
        Map<Double, Integer> frequencyMap = new HashMap<>();

        for (Text value : values) {
            double columnValue = Double.parseDouble(value.toString());

            max = Math.max(max, columnValue);
            min = Math.min(min, columnValue);
            sum += columnValue;
            count++;

            frequencyMap.put(columnValue, frequencyMap.getOrDefault(columnValue, 0) + 1);
        }

        double mean = sum / count;
        double mode = getMode(frequencyMap);

        context.write(key, new Text(String.format("Max: %.2f, Min: %.2f, Mean: %.2f, Mode: %.2f", max, min, mean, mode)));
    }

    public double getMode(Map<Double, Integer> frequencyMap) {
        double mode = 0;
        int maxCount = 0;

        for (Map.Entry<Double, Integer> entry : frequencyMap.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                mode = entry.getKey();
            }
        }

        return mode;
    }
}

