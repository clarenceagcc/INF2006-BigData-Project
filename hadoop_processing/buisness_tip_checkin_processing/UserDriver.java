package yelpuserdataset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UserDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: UserDriver <input path 1> <input path 2> ... <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Yelp Data Preprocessing - Tip, Check-in, Business");

        job.setJarByClass(UserDriver.class);
        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add all input paths except the last one (last is output path)
        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        // Add named outputs for MultipleOutputs
        MultipleOutputs.addNamedOutput(job, "checkin", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "tip", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "business", TextOutputFormat.class, Text.class, Text.class);

        // Submit job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}