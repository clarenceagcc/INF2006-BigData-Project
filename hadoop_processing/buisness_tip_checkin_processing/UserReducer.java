package yelpuserdataset;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;

public class UserReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> checkinTimestamps = new HashSet<>();
        HashSet<String> tipEntries = new HashSet<>();
        HashSet<String> businessEntries = new HashSet<>();

        for (Text value : values) {
            String valStr = value.toString();
            String[] parts = valStr.split("\t", 2);
            if (parts.length < 2) continue;

            String type = parts[0];
            String data = parts[1];

            switch (type) {
                case "CHECKIN":
                    String[] timestamps = data.split(",\\s*");
                    for (String ts : timestamps) {
                        checkinTimestamps.add(ts.trim());
                    }
                    break;

                case "TIP":
                    tipEntries.add(data);
                    break;

                case "BUSINESS":
                    businessEntries.add(data);
                    break;
            }
        }

        // Emit deduplicated check-in timestamps
        if (!checkinTimestamps.isEmpty()) {
            StringBuilder checkins = new StringBuilder();
            for (String ts : checkinTimestamps) {
                if (checkins.length() > 0) checkins.append(", ");
                checkins.append(ts);
            }
            multipleOutputs.write("checkin", key, new Text(checkins.toString()));
        }

        // Emit deduplicated tip entries
        for (String tip : tipEntries) {
            multipleOutputs.write("tip", key, new Text(tip));
        }

        // Emit business data (deduplicated, usually only 1 per business_id)
        for (String business : businessEntries) {
            multipleOutputs.write("business", key, new Text(business));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
