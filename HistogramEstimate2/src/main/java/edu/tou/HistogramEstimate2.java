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

public class HistogramEstimate2 {

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

	public static class HistogramEstimateReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>
	{
		private LongWritable keyOut;
		private LongWritable valueOut;

		private long N;
		private long B;
		private long T;
		private long K;
		private long t;
		private long k;
		private long s;
		private long kx;
		private long sx;
		private long nBuckets;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			keyOut = new LongWritable();
			valueOut = new LongWritable();

			B = Long.parseLong(context.getConfiguration().get("B"));
			T = Long.parseLong(context.getConfiguration().get("T"));
			N = 0;
			t = 0;
			k = 0;
			s = 0;
			kx = 0;
			sx = 0;
		}

		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			System.out.println("Key: " + key.get());

			if(key.get() == Long.MIN_VALUE) {
				for(LongWritable value : values) {
					K += 1;
					N += value.get();
				}
				nBuckets = K * (T + 1) - 1;
			} else {
				for (LongWritable value : values) {
					if (s+sx >= (t*N)/B) {
						if (t > 0) {
							keyOut.set(k);
							valueOut.set(sx);
							context.write(keyOut, valueOut);
						}
						k = key.get();
						t += 1;
						s += sx;
						sx = 0;
					}
					sx += value.get();
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			keyOut.set(k);
			valueOut.set(sx);
			context.write(keyOut, valueOut);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {

		final long timeOfOneDay = 1000*60*60*24;

		if (args.length < 5) {
			System.err.println("Usage: <B> <T> <startDate> <endDate> <inputFile> <outputFile>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		int B = Integer.parseInt(args[0]);
		int T = Integer.parseInt(args[1]);

		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Date startDate = dateFormat.parse(args[2]);
		Date endDate = dateFormat.parse(args[3]);

		String inputFile = args[4];
		String outputFile = args[5];

		conf.set("B", Integer.toString(B));
		conf.set("T", Integer.toString(T));

		Job job = new Job(conf, "Histogram Estimate");
		job.setJarByClass(HistogramEstimate2.class);
		job.setMapperClass(HistgramEstimateMapper.class);
		job.setReducerClass(HistogramEstimateReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.getConfiguration().set("mapreduce.output.basename", outputFile);


		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		long time = startTime;
		FileSystem fileSystem = FileSystem.get(conf);
		while(time <= endTime) {
			Path path = new Path(inputFile + "/" + dateFormat.format(new Date(time)));
			if(fileSystem.exists(path)) {
				FileInputFormat.addInputPath(job, path);
			}
			time += timeOfOneDay;
		}
		fileSystem.delete(new Path(outputFile), true);
		FileOutputFormat.setOutputPath(job,new Path(outputFile));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
