package clustering.kmeans.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

/**
 * Klasa odpowiedzialna za inicjalizacje klastrow na podstawie pliku wczytanego z distributed cache.
 * Podczas konfiguracji zostaja wczytane numery klastrow. Oznaczaja one numery rektordow, ktore 
 * zostana uzyte jako pierwszy zestaw klastrow.
 * @author Maciej Mazur
 *
 */
public class ClustersRandomInitialization {
	/**
	 * Sciezka do pliku w distributed cache.
	 */
	private static final String HDFS_CLUSTER_INITIALIZATION_CACHE = "/data/initial_clusters_numbers.txt";

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable one = new IntWritable(1);

		private HashSet<Long> selectedIndexes = new HashSet<Long>();

		/**
		 * Mapper filtruje wybrane wartosci, ktore zostana pozniej uzyte jako srodki klastrow.
		 */
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> out, Reporter arg3)
				throws IOException {

			String line = value.toString();
			
			Vector v = Vector.createVector(line);

			if (v != null && selectedIndexes.contains(v.getNr())) {
				out.collect(one, new Text(line.substring(line.indexOf(" ")+1)));
			}

		}

		/**
		 * Wczytanie pliku z numerami klastrow z distributed cache.
		 */
		@Override
		public void configure(JobConf job) {
			System.out.println("CONFIGURE");
			String cache = new Path(HDFS_CLUSTER_INITIALIZATION_CACHE)
					.getName();
			try {
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
				FileSystem fs = FileSystem.getLocal(job);
				if (null != cacheFiles && cacheFiles.length > 0) {
					for (Path cachePath : cacheFiles) {
						System.out.println(cachePath.getName());
						if (cachePath.getName().equals(cache)) {
							loadCache(cachePath, fs);
							break;
						}
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e.getCause());
			}
		}

		void loadCache(Path cachePath, FileSystem fs) throws IOException {
			System.out.println("Load Cache: "+cachePath.getName());
			BufferedReader wordReader = new BufferedReader(new FileReader(
					cachePath.toString()));
			try {
				String line;
				while ((line = wordReader.readLine()) != null) {
					selectedIndexes.add(Long.parseLong(line));
				}
			} finally {
				wordReader.close();
			}
		}

	}

	/**
	 * Reduce przeglada wszystkie wartosci i nadaje im kolejne numery, ktore stana sie pozniej numerami klastrow.
	 * @author Maciej Mazur
	 *
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter arg3)
				throws IOException {
			int i = 0;

			while (values.hasNext()) {
				Text t = values.next();
				output.collect(new IntWritable(i), t);
				i++;
			}

		}

	}

	private int clusters;
	private long records;
	private String input;
	private String output;

	public ClustersRandomInitialization(int clusters, long records,
			String input, String output) {
		this.clusters = clusters;
		this.input = input;
		this.output = output;
		this.records = records;
	}

	/**
	 * Urochomienie zadania odpowiedzialnego za utworzenie poczatkowych klastrow.
	 * Wystepuje tylko jedno zadanie typu Reduce.
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws URISyntaxException
	 */
	public int execute() throws IOException, InterruptedException,
			ClassNotFoundException, URISyntaxException {
		JobConf conf = new JobConf(ClustersRandomInitialization.class);
		conf.setJobName("########################################################### Select random initial clusters: "
				+ clusters);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		initializeCache(conf);

		Job job = Job.getInstance(conf);
		job.submit();
		return job.waitForCompletion(true) ? 0 : 1;
	}

	
	/**
	 * Inicjalizacja pliku z numerami rektortdow ktore zostana pozniej uzyte jako srodki klastrow.
	 * @param conf
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	private void initializeCache(JobConf conf) throws IOException,
			URISyntaxException {
		Path cache = new Path(HDFS_CLUSTER_INITIALIZATION_CACHE);
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream strm = fs.create(cache);

		TreeSet<Long> set = new TreeSet<Long>();
		Random random = new Random();
		while (set.size() < clusters) {
			set.add(Math.abs(random.nextLong()) % records);
		}

		for (Long i : set) {
			strm.writeBytes(String.valueOf(i) + "\n");
		}
		strm.flush();
		strm.close();
		DistributedCache.addCacheFile(
				new URI(HDFS_CLUSTER_INITIALIZATION_CACHE), conf);
	}

}
