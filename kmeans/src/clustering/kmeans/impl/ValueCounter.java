package clustering.kmeans.impl;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;


public class ValueCounter {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, NullWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable,NullWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			if(line != null) {
				Counter counter = reporter.getCounter(RecordsCounter.Records);
				counter.increment(1);
			}
		}
	}

	public long countValues(String inputPath, String outputPath) throws Exception {
		JobConf conf = new JobConf(KMeans.class);
		conf.setJobName("########################################################### Count rows in data set");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		Job job = Job.getInstance(conf);
		job.setNumReduceTasks(0);
		
		job.submit();
		int code = job.waitForCompletion(true) ? 0 : 1;
		FileSystem.get(conf).delete(new Path(outputPath), true);
		if(code == 0){
			Counter counter = job.getCounters().findCounter(RecordsCounter.Records);
			return counter.getValue();}
		else {
			throw new Exception("Completion code == 0");
		}
		
	}
}
