import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CountSum {
    private static class CountSumMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String count = value.toString().split("\t")[1];
            context.write(new Text("summary"), new Text(count));
        }
    }

    private static class CountSumReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(Text value : values){
                sum += Integer.valueOf(value.toString());
            }
            context.write(new Text("result"), new Text("" + sum / 3));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job1 = Job.getInstance(conf, "Sum");
        job1.setJarByClass(CountSum.class);
        job1.setMapperClass(CountSumMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(CountSumReducer.class);
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1, input);
        FileOutputFormat.setOutputPath(job1, output);

        job1.waitForCompletion(true);
    }
}
