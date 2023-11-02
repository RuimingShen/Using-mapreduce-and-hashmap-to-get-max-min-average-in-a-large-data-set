import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        boolean isValidRow = false;

        for (String val : values) {
            double columnValue;

            try {
                columnValue = Double.parseDouble(val);
            } catch (NumberFormatException e) {
                continue;
            }

            if (columnValue != 0.0) {
                isValidRow = true;
                break;
            }
        }

        if (!isValidRow) {
            context.getCounter("INVALID_ROWS", "AllZerosAndNulls").increment(1);
            return;
        }

        for (int n = 1; n <= values.length / 6; n++) {
            int colIdx = 6 * n - 1;

            if (colIdx < values.length) {
                double columnValue;

                try {
                    columnValue = Double.parseDouble(values[colIdx]);
                } catch (NumberFormatException e) {
                    continue;
                }

                if (n % 3 == 0 || n % 4 == 0 || n % 5 == 0) {
                    columnValue = new BigDecimal(columnValue).setScale(2, RoundingMode.HALF_UP).doubleValue();
                }

                context.write(new Text(Integer.toString(n)), new Text(Double.toString(columnValue)));
            }
        }
    }
}


