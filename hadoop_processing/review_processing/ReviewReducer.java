import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ReviewReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Calculate the partition based on the key (or other criteria)
        int partition = Math.abs(key.toString().hashCode()) % 12;  // 12 partitions

        // Write the data to the corresponding output file
        String outputName = "output" + partition;
        
        // Debug output to check which partition the data is being written to
        System.out.println("Key: " + key.toString() + " --> Writing to: " + outputName);
        
        for (Text value : values) {
            multipleOutputs.write(key, value, outputName);  // Write to the appropriate output
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}