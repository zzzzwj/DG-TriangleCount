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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class AdjTable {

    private static class AdjTableMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(" ");
            if (values[0] != values[1])
                context.write(new Text(values[0]), new Text(values[1]));
        }
    }

    private static class AdjTableReducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            StringBuilder sb = new StringBuilder();
            HashMap<String, Integer> pointmap = new HashMap<>();
            for(Text value : values){
                if(pointmap.containsKey(value.toString()))
                    continue;
                pointmap.put(value.toString(), 1);
                sb.append(value + ",");
            }
            mos.write(key, new Text(sb.toString()), key.charAt(0)+"");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
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

        Job job1 = Job.getInstance(conf, "AdjTable");
        job1.setJarByClass(AdjTable.class);
        job1.setMapperClass(AdjTableMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(AdjTableReducer.class);
        job1.setNumReduceTasks(Integer.valueOf(args[2]));
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1, input);
        FileOutputFormat.setOutputPath(job1, output);
        job1.waitForCompletion(true);
    }
}
