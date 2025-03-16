import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SemiJoinDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "SemiJoin with Bloom Filter");
        job.setJarByClass(SemiJoinDriver.class);

        // Set Mapper and Reducer
        job.setMapperClass(SemiJoinMapper.class);
        job.setReducerClass(SemiJoinReducer.class);

        // Set Output Key-Value Types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Set Input & Output Format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
