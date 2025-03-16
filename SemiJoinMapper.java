import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.*;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import java.io.*;

public class SemiJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private BloomFilter filter = new BloomFilter();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        // Read Bloom Filter from Distributed Cache
        FSDataInputStream in = fs.open(new Path("bloomfilter"));
        filter.readFields(in);
        in.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String joinKey = fields[0]; // Assume first field is the join key

        if (filter.membershipTest(new Key(joinKey.getBytes()))) {
            context.write(new Text(value), NullWritable.get());
        }
    }
}
