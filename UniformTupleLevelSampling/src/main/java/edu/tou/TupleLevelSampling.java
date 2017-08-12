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
import java.net.URISyntaxException;
import java.util.Random;

/**
 * Created by tolga on 12/8/15.
 */
public class TupleLevelSampling {

    public static class HistogramSummarizeMapper extends Mapper<Object,Text,LongWritable,LongWritable>
    {
        private Random rands = new Random();
        private Double percentage;
        private LongWritable keyOut;
        private LongWritable valueOut;
        private String column;
        private long i;
        private int c;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            keyOut = new LongWritable();
            valueOut=new LongWritable(0);
            column = context.getConfiguration().get("column");
            i=0;
            c=0;
            String strPercentage = context.getConfiguration().get(
                    "percentage");
            percentage = Double.parseDouble(strPercentage) / 100.0;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            keyOut.set(Long.MIN_VALUE);
            valueOut.set(i);
            context.write(keyOut,valueOut);

        }


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (rands.nextDouble() < percentage) {
                String[] line = (value.toString()).split(" ");
                try {
                    if (line.length == 4) {
                        c = Integer.parseInt(column);
                        keyOut.set(Long.parseLong(line[c]));
                        i++;
                        context.write(keyOut, valueOut);
                    }

                } catch (NumberFormatException e) {
                    System.err.println(e);
                }

            }
        }
    }

    public static class HistogramSummarizeReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>
    {
        private LongWritable keyOut;
        private LongWritable valueOut;

        private long numberOfTuples;
        private long numberOfBuckets;
        private long limitOfBucketTuples;
        private long numberOfBucketTuples;
        private long numberOfSeenTuples;
        private long numberOfWrittenBuckets;
        private long keyOfFirstBucketTuple;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {


            keyOut   = new LongWritable();
            valueOut = new LongWritable();

            numberOfTuples  = 0;
            numberOfBuckets = Long.parseLong(context.getConfiguration().get("numberOfBuckets"));

            limitOfBucketTuples   = 0;
            numberOfBucketTuples  = 0;
            numberOfSeenTuples    = 0;
            numberOfWrittenBuckets = 0;
            keyOfFirstBucketTuple = 0;

        }

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            for(LongWritable value : values)
            {
                if(key.get()==Long.MIN_VALUE)
                {
                    numberOfTuples+=value.get();
                    limitOfBucketTuples   = numberOfTuples / numberOfBuckets;
                }
                else
                {
                    if(numberOfBucketTuples == 0)
                    {
                        keyOfFirstBucketTuple = key.get();
                    }

                    numberOfBucketTuples ++;
                    numberOfSeenTuples ++;

                    if(numberOfWrittenBuckets < numberOfBuckets-1)
                    {
                        limitOfBucketTuples=numberOfTuples*(numberOfWrittenBuckets+1)/numberOfBuckets;
                        if(numberOfSeenTuples >= limitOfBucketTuples)
                        {
                            keyOut.set(keyOfFirstBucketTuple);
                            valueOut.set(numberOfBucketTuples);
                            context.write(keyOut, valueOut);

                            numberOfWrittenBuckets ++;
                            numberOfBucketTuples = 0;
                        }
                    }
                    else
                    {
                        if(numberOfSeenTuples == numberOfTuples)
                        {
                            keyOut.set(keyOfFirstBucketTuple);
                            valueOut.set(numberOfBucketTuples);
                            context.write(keyOut, valueOut);
                            numberOfWrittenBuckets ++;

                            keyOut.set(key.get());
                            valueOut.set(0);
                            context.write(keyOut, valueOut);

                            keyOut.set(Long.MIN_VALUE);
                            valueOut.set(numberOfTuples);
                            context.write(keyOut, valueOut);
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        if (args.length < 5)
        {
            System.out.println("Args length:"+args.length);
            System.err.println("Usage: <column> <percentage> <numberOfBuckets> <inputFile> <outputFile>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        String column = args[0];
        String percentage=args[1];
        long numberOfBuckets = Long.parseLong(args[2]);

        String inputFile = args[3];
        String outputFile = args[4];

        conf.set("column", column);
        conf.set("percentage",percentage);
        conf.set("numberOfBuckets", Long.toString(numberOfBuckets));
        Job job = new Job(conf, "Histogram Summarize");

        job.setJarByClass(TupleLevelSampling.class);
        job.setMapperClass(HistogramSummarizeMapper.class);
        job.setReducerClass(HistogramSummarizeReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(new Path(outputFile)))
        {
            fileSystem.delete(new Path(outputFile));
            fileSystem.getLocal(conf).delete(new Path(outputFile), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
