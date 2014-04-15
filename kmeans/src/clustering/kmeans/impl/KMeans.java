package clustering.kmeans.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class KMeans {

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.out
					.println("Usage: \n hadoop ... input_path output_path clusters max_iterations");
			return;
		}
		String input = args[0];
		String output = args[1];
		String countOutput = output + "_count";
		String clustersInitialPath = output + "_0";
		int clusters = Integer.parseInt(args[2]);
		int iterations = Integer.parseInt(args[3]);

		ValueCounter counter = new ValueCounter();
		long records = counter.countValues(input, countOutput);
		
		System.out.println("########################################################### COUNTED RECORDS: " + records);
		ClustersRandomInitialization initialization = new ClustersRandomInitialization(
				clusters, records, input, clustersInitialPath);

		int initRandom = initialization.execute();
		if(initRandom!=0){
			throw new Exception("########################################################### Errior during initialization");
		}

		
		Clustering clustering = new Clustering(input, output, clustersInitialPath,iterations);
		
		clustering.run();
		

	}
}