package clustering.kmeans.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import clustering.kmeans.datatypes.Vector;
import clustering.kmeans.impl.ClustersRandomInitialization.Map;
import clustering.kmeans.impl.ClustersRandomInitialization.Reduce;

public class Clustering {

	public static final String CURRENT_PATH_CLUSTER_PROPERTY = "current.clusters";
	private String input;
	private String output;
	private String clustersInitialPath;
	private int iterations;

	public void run() throws Exception {
		int it = 1;

		String prev = clustersInitialPath;
		while (it <= iterations) {
			String outputPath = output + "_" + it;
			JobConf conf = new JobConf(KMeans.class);
			conf.setJobName("########################################################### KMEANS CLUSTERING ITERATION: "
					+ it);
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			conf.setInputFormat(TextInputFormat.class);

			conf.set(CURRENT_PATH_CLUSTER_PROPERTY, prev);

			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(outputPath));

			Job job = Job.getInstance(conf);
			job.submit();
			if (!job.waitForCompletion(true)) {
				throw new Exception("Error during Clustering, iteration: " + it);
			}

			prev = outputPath;
			it++;
		}
	}

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable one = new IntWritable(1);

		private ArrayList<Vector> clusters = new ArrayList<Vector> ();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> out, Reporter arg3)
				throws IOException {

			String line = value.toString();

			Vector v = Vector.createVector(line);

		}

		@Override
		public void configure(JobConf job) {
			System.out.println("CONFIGURE");
			
			String clustersPath = job.get(CURRENT_PATH_CLUSTER_PROPERTY);
			try {
				FileSystem fs = FileSystem.get(new Path(clustersPath).toUri(),job);
				load(new Path(clustersPath), fs,job);
			} catch (IOException e) {
				throw new RuntimeException(e.getCause());
			}
		}

		void load(Path path, FileSystem fs,JobConf job) throws IOException {
			System.out.println("Loading Clusters: " + path.getName());
			FileStatus[] statuses = fs.listStatus(path,new PathFilter() {
                public boolean accept(Path path) {
                    return path.toString().contains("part-");
                 }
              });
			
			for(FileStatus status : statuses){
				System.out.println(status.getPath().toString());
				
				 InputStream stream = fs.open(status.getPath());
				 
					BufferedReader wordReader = new BufferedReader(new InputStreamReader(stream));
					try {
						String line;
						while ((line = wordReader.readLine()) != null) {
							System.out.println("ReadLine: "+line);
						}
					} finally {
						wordReader.close();
					}
			}

		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter arg3)
				throws IOException {
			long count=0;
			
			Vector sum = null;
			while (values.hasNext()) {
				Text t = values.next();
				Vector v = Vector.createVectorWithoutNr(t.toString());
				if(sum == null){
					sum = v;
				} else {
					sum.add(v);
				}
				count = count + 1;
			}
			
			if(sum!=null){
				sum.multiply(1/(double)count);
				output.collect(key, new Text(sum.toStringWithoutNr()));
			}
			
			
		}

	}

	public Clustering(String input, String output, String clustersInitialPath,
			int iterations) {

		this.input = input;
		this.output = output;
		this.clustersInitialPath = clustersInitialPath;
		this.iterations = iterations;
	}

}
