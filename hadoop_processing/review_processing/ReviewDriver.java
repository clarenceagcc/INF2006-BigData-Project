import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ReviewDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: YelpReviewPreprocessDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Yelp Review Data Preprocessing");

        job.setJarByClass(ReviewDriver.class);
        job.setMapperClass(ReviewMapper.class);
        job.setReducerClass(ReviewReducer.class);
        job.setPartitionerClass(CustomPartitioner.class); // Use custom partitioner

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // Set the base output path (ensure it does not overwrite the multiple outputs)
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/output0"));

        // Add multiple outputs (12 outputs)
        for (int i = 0; i < 12; i++) {
            MultipleOutputs.addNamedOutput(job, "output" + i, TextOutputFormat.class, Text.class, Text.class);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}