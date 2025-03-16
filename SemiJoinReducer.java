import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class SemiJoinReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
