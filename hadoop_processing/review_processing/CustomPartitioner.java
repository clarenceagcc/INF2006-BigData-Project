import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, Text> {
    private static final int NUM_PARTITIONS = 12;  // Update to 12 partitions

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        int partition = Math.abs(key.toString().hashCode()) % NUM_PARTITIONS;
        
        // Debug output to check partition calculation
        System.out.println("Key: " + key.toString() + " --> Partition: " + partition);
        
        return partition;
    }
}
