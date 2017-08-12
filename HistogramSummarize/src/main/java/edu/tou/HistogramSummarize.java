package edu.tou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HistogramSummarize {


    public static class HistogramSummarizeMapper extends Mapper<Object,Text,LongWritable,LongWritable>
	{
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
		}

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
			keyOut.set(Long.MIN_VALUE);
			valueOut.set(i);
            context.write(keyOut,valueOut);

        }


        @Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String [] line= (value.toString()).split(" ");
				try
				{
					if(line.length==4)
					{
						c=Integer.parseInt(column);
						keyOut.set(Long.parseLong(line[c]));
						i++;
						context.write(keyOut, valueOut);
                    }

				}
				catch (NumberFormatException e)
				{
					System.err.println(e);
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
		if (args.length < 4)
		{
			System.out.println("Args length:"+args.length);
			System.err.println("Usage: <column> <numberOfBuckets> <inputFile> <outputFile>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		String column = args[0];
		long numberOfBuckets = Long.parseLong(args[1]);

		String inputFile = args[2];
		String outputFile = args[3];

		conf.set("column", column);
		conf.set("numberOfBuckets", Long.toString(numberOfBuckets));
		Job job = new Job(conf, "Histogram Summarize");

		job.setJarByClass(HistogramSummarize.class);
		job.setMapperClass(HistogramSummarizeMapper.class);
		job.setReducerClass(HistogramSummarizeReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.getConfiguration().set("mapreduce.output.basename", inputFile);

		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(new Path(outputFile)))
		{
			fileSystem.delete(new Path(outputFile));
			fileSystem.getLocal(conf).delete(new Path(outputFile), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		if(job.waitForCompletion(true))
		{
			FileStatus[] fileStatusArray = fileSystem.listStatus(new Path(outputFile));
			String pattern=job.getConfiguration().get("mapreduce.output.basename")+"/*";
			Pattern regex = Pattern.compile(pattern);
			String baseName=job.getConfiguration().get("mapreduce.output.basename");
			if(fileStatusArray!=null)
			{
				for(FileStatus fileStatus:fileStatusArray)
				{
					String outputFileName=fileStatus.getPath().getName();
					Matcher matcher = regex.matcher(outputFileName);
					if(matcher.find())
					{
						System.out.println(baseName);
						fileSystem.copyToLocalFile(new Path(outputFile + "/" + outputFileName), new Path("Summaries/"+baseName));
					}

				}
			}
		}
		System.exit(job.isSuccessful() ? 0 : 1);

	}
}