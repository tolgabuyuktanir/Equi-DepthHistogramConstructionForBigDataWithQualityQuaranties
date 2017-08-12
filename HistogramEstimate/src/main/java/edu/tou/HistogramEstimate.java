package edu.tou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class HistogramEstimate {

	public static class HistgramEstimateMapper extends Mapper<Object,Text,LongWritable,LongWritable>
	{
		private LongWritable keyOut;
		private LongWritable valueOut;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			keyOut = new LongWritable();
			valueOut = new LongWritable();
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{	
			StringTokenizer st = new StringTokenizer(value.toString());
			keyOut.set(Long.parseLong(st.nextToken()));
			valueOut.set(Long.parseLong(st.nextToken()));
			context.write(keyOut, valueOut);
		}
	}

	public static class HistogramEstimateReducer extends Reducer<LongWritable,LongWritable,LongWritable,NullWritable>
	{
		private LongWritable keyOut;

		private long numberOfTuples;
		private long numberOfBuckets;
		private long numberOfSummaries;
		private long numberOfSummaryTuples;
		private long limitOfBucketTuples;
		private long numberOfWrittenBuckets;
		private long numberOfWrittenSummaries;
		private long limitOfNumberOfSeenTuples;
		private long upperBoundOnNumberOfSeenTuples;
		private long keyOfFirstBucketTuple;
		private long keyOfLastTuple;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			keyOut   = new LongWritable();

			numberOfBuckets   = Long.parseLong(context.getConfiguration().get("numberOfBuckets"));
			numberOfTuples    = 0;
			numberOfSummaries = 0;
			numberOfSummaryTuples = 0;
			limitOfBucketTuples = 0;
			numberOfWrittenBuckets = 0;
			numberOfWrittenSummaries = 0;
			limitOfNumberOfSeenTuples = 0;
			upperBoundOnNumberOfSeenTuples = 0;
			keyOfFirstBucketTuple = 0;
			keyOfLastTuple = 0;
		}

		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			System.out.println("Key: " + key.get());

			if(key.get() == Long.MIN_VALUE)
			{
				for(LongWritable value : values)
				{
					numberOfSummaryTuples = value.get();
					numberOfTuples += numberOfSummaryTuples;
					numberOfSummaries ++;
				}

				limitOfBucketTuples = numberOfTuples / numberOfBuckets;
				limitOfNumberOfSeenTuples = limitOfBucketTuples;
			}
			else
			{
				for (LongWritable value : values) {
					if (upperBoundOnNumberOfSeenTuples == 0) {
						keyOfFirstBucketTuple = key.get();
					}

					if (value.get() > 0) {
						upperBoundOnNumberOfSeenTuples += value.get();
					} else {
						numberOfWrittenSummaries++;
						keyOfLastTuple = key.get();
					}

					if (numberOfWrittenBuckets < numberOfBuckets - 1) {
						if (upperBoundOnNumberOfSeenTuples > limitOfNumberOfSeenTuples) {
							keyOut.set(keyOfFirstBucketTuple);
							context.write(keyOut, NullWritable.get());

							numberOfWrittenBuckets++;
							limitOfNumberOfSeenTuples += limitOfBucketTuples;
							keyOfFirstBucketTuple = key.get();
						}
					} else {
						if (numberOfWrittenSummaries == numberOfSummaries) {
							keyOut.set(keyOfFirstBucketTuple);
							context.write(keyOut, NullWritable.get());

							keyOut.set(keyOfLastTuple);
							context.write(keyOut, NullWritable.get());
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {

		final long timeOfOneDay = 1000*60*60*24;

		if (args.length < 5)
		{
			System.err.println("Usage:<numberOfBuckets> <startDate> <endDate> <inputFile> <outputFile>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		int numberOfBuckets = Integer.parseInt(args[0]);

		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Date startDate = dateFormat.parse(args[1]);
		Date endDate = dateFormat.parse(args[2]);

		long startTime = startDate.getTime();
		long endTime = endDate.getTime();

		String inputFile = args[3];
		String outputFile = args[4];

		conf.set("numberOfBuckets", Integer.toString(numberOfBuckets));
		Job job = new Job(conf, "Histogram Estimate");
		
		job.setJarByClass(HistogramEstimate.class);
		job.setMapperClass(HistgramEstimateMapper.class);
		job.setReducerClass(HistogramEstimateReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.getConfiguration().set("mapreduce.output.basename", outputFile);


		long time = startTime;
		FileSystem fileSystem = FileSystem.get(conf);
		while(time <= endTime)
		{
			Path path = new Path(inputFile + "/" + dateFormat.format(new Date(time)));
			if(fileSystem.exists(path))
			{
				FileInputFormat.addInputPath(job, path);
			}
			time += timeOfOneDay;
		}
		fileSystem.delete(new Path(outputFile), true);
		FileOutputFormat.setOutputPath(job,new Path(outputFile));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
