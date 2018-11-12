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
import java.util.ArrayList;
import java.util.HashMap;

public class TriangleCount {

    private static class TC_Stage_1_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vs = value.toString().split("\t");
            String srcPoint = vs[0];
            String[] points = vs[1].split(",");
            for (String point : points) {
                context.write(new Text(point), new Text(srcPoint));
            }
            context.write(new Text(srcPoint), new Text("#" + vs[1]));
        }
    }

    private static class TC_Stage_1_Reducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            String table = new String();
            for (Text value : values) {
                if (!value.toString().startsWith("#"))
                    sb.append(value + ";");
                else
                    table = value.toString();
            }
            sb.append(table);
            mos.write(new Text(key), new Text(sb.toString()), key.charAt(0) + "");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    private static class TC_Stage_2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            String srcPoint = values[0];
            String[] edges = values[1].split(";");
            String table = edges[edges.length - 1];
            if (table.charAt(0) != '#')
                return;
            else
                table = table.substring(1);
            String[] dest_points = table.split(",");

            for (String dest_point : dest_points) {
                for (int i = 0; i < edges.length - 1; i++) {
                    if (!edges[i].equals(dest_point)) {
                        context.write(new Text(dest_point), new Text(edges[i] + "," + srcPoint));
                    }
                }
            }
            context.write(new Text(srcPoint), new Text("#" + table));
        }
    }

    private static class TC_Stage_2_Reducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String table = null;
            ArrayList<String> contents = new ArrayList<String>();
            for (Text value : values) {
                if (value.charAt(0) == '#') {
                    table = value.toString().substring(1);
                } else {
                    contents.add(value.toString());
                }
            }
            if (table == null)
                return;
            String[] dest_points = table.split(",");
            HashMap<String, Integer> pointmap = new HashMap<String, Integer>();
            for (String dest_point : dest_points) {
                pointmap.put(dest_point, 1);
            }
            for (String value : contents) {
                if (value.charAt(0) != '#') {
                    String startpoint = value.split(",")[0];
                    if (pointmap.containsKey(startpoint)) {
                        sum++;
                    }
                }
            }
            mos.write(key, new Text("" + sum), key.charAt(0) + "");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path temp_out = new Path("temp/");

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        if (hdfs.exists(temp_out)) {
            hdfs.delete(temp_out, true);
        }

        Job job1 = Job.getInstance(conf, "Count1");
        job1.setJarByClass(TriangleCount.class);
        job1.setMapperClass(TC_Stage_1_Mapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(TC_Stage_1_Reducer.class);
        job1.setNumReduceTasks(Integer.valueOf(args[2]));
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1, input);
        FileOutputFormat.setOutputPath(job1, temp_out);

        Job job2 = Job.getInstance(conf, "Count2");
        job2.setJarByClass(TriangleCount.class);
        job2.setMapperClass(TC_Stage_2_Mapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(TC_Stage_2_Reducer.class);
        job2.setNumReduceTasks(Integer.valueOf(args[2]));
        LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, temp_out);
        FileOutputFormat.setOutputPath(job2, output);


        job1.waitForCompletion(true);
        job2.waitForCompletion(true);

        hdfs.delete(temp_out, true);
    }
}
