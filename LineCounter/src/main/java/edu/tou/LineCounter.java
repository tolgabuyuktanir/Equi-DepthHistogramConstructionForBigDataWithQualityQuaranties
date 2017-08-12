package edu.tou;

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

import java.io.IOException;

/**
 * Created by tolga on 10/17/15.
 */
public class LineCounter {
    public static class Map extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private String column;
        private int c=0;
        private long l=0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            column = context.getConfiguration().get("column");
            c=Integer.parseInt(column);
        }
        int i=0;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String [] line=value.toString().split(" ");
            if(line[c]!=null)
            {
                try
                {
                    l=Long.parseLong(line[c]);
                    context.write(new Text("Total Line"), one);
                }
                catch (NumberFormatException e)
                {
                    System.out.println(e);
                }

            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        long sum=0;
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            for(LongWritable val:values)
            {
                sum+=val.get();
            }
            context.write(new Text("Total Line"), new LongWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String column = args[0];
        conf.set("column", column);
        Job job = new Job(conf, "LineCount");
        job.setJarByClass(LineCounter.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        //job.setReducerClass(Reduce.class);

        FileSystem fileSystem= FileSystem.get(conf);
        fileSystem.delete(new Path(args[2]),true);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);

    }
}
