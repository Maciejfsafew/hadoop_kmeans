package clustering.kmeans.impl;

import java.io.BufferedReader;
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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import clustering.kmeans.datatypes.Vector;
import clustering.kmeans.distance.DistanceMeasure;
import clustering.kmeans.distance.impl.Euclidean;

/**
 * Glowna klasa uruchamiajaca klasteryzacje.
 * 
 * @author Maciej Mazur
 *
 */
public class Clustering {

	/**
	 * Plik zawierajcy klastry utworzone w poprzedniej iteracji.
	 */
	public static final String CURRENT_PATH_CLUSTER_PROPERTY = "current.clusters";
	/**
	 * Sciezka do pliku z danymi do klasteryzacji
	 */
	private String input;
	/**
	 * Prefiks sciezki do pliku do ktorego zostana zapisane oblcizone klastry.
	 */
	private String output;
	
	/**
	 * Sciezka do pliku z losowo zainicjalizowanymi klastrami.
	 */
	private String clustersInitialPath;
	/**
	 * Odgorne ogranicznie na liczbe iteracji.
	 */
	private int iterations;
	/**
	 * Blad otrzymany po wszystkich wykonanych iteracjach.
	 */
	private ArrayList<Long> errors = new ArrayList<Long>();

	private int it = 1;

	private final double errorThreashold = 0.0001;
	/**
	 * Metoda odpowiedzialne za odpalanie kolejnych iteracji algorytmu.
	 * W kazym kroku tworzony jest nowy plik z obliczonymi klastrami, ktory zostanie uzyty w kolejnej iteracji.
	 * Funkcja zostaje przerwana w dwoch przypadkach:
	 * - zostanie osiagnieta maksymala liczba iteracji.
	 * - blad z poprzedniej iteracji bedzie mniejszy niz 10% calkowitego bledu z poprzedniej iteracji.
	 * @throws Exception
	 */
	public void run() throws Exception {

		String prev = clustersInitialPath;
		Long prevError = Long.MAX_VALUE;
		
		while (it <= iterations) {
			
			String outputPath = output + "_" + it;
			JobConf conf = new JobConf(KMeans.class);
			conf.setJobName("########################################################### KMEANS CLUSTERING ITERATION: "
					+ it);
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			conf.setCombinerClass(Combiner.class);
			conf.setInputFormat(TextInputFormat.class);

			conf.set(CURRENT_PATH_CLUSTER_PROPERTY, prev);

			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(outputPath));

			Job job = Job.getInstance(conf);
			job.submit();
			if (!job.waitForCompletion(true)) {
				throw new Exception("Error during Clustering, iteration: " + it);
			}

			Counter counter = job.getCounters().findCounter(
					RecordsCounter.Records);
			errors.add(counter.getValue());
			long error = counter.getValue();
			prev = outputPath;
			it++;
			
			if(Math.abs(prevError - error) < errorThreashold * prevError){
				break;
			}
			prevError = error;
		}
	}
	/**
	 * Mapper odpowiedzialny jest za wykonanie trzech operacji:
	 * - wczytanie danych z pliku umieszczonego w systemie plikow hdfs.
	 * - przypisanie wektorom z danymi do najblizszego klastra.
	 * - obliczenie funkcji kosztu.
	 * Rezultatem mappingu jest klucz, ktory reprezentuje numer najblizszego klastra oraz
	 * wektor danych bedacy kopia wektora danych.
	 * @author Maciej Mazur
	 *
	 */
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {

		/**
		 * Struktura zawierajaca klastry.
		 */
		private ArrayList<Vector> clusters = new ArrayList<Vector>();
		/**
		 * Metryka oleglosci miedzy dwoama wektorami
		 */
		private DistanceMeasure measure;

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> out, Reporter reporter)
				throws IOException {

			String line = value.toString();

			Vector v = Vector.createVector(line);
			Vector v2 = null;
			double distanceMax = Double.MAX_VALUE;
			for (Vector vec : clusters) {
				double distance = measure.distance(vec, v);
				if (distance < distanceMax) {
					distanceMax = distance;
					v2 = vec;
				}

			}

			/**
			 * Obliczanie wartosci kosztu dla poprzedniej iteracji.
			 */
			Counter counter = reporter.getCounter(RecordsCounter.Records);
			counter.increment((long) (measure.distance(v2, v)));
			/**
			 * Ustawienie zaagregowanej wartosci ilosci elementow w sredniej.
			 */
			v.setNr(1);
			if (v2 != null) {
				out.collect(new IntWritable((int) v2.getNr()),
						new Text(v.toString()));
			}

		}

		/**
		 * Inicjalizacja struktury danych reprezentujacej klastry przed uruchomieniem mappera.
		 * Zostaje wczytany plik zawierajacy klastry obliczone w poprzedniej iteracji.
		 */
		@Override
		public void configure(JobConf job) {
			System.out.println("CONFIGURE");

			measure = new Euclidean();

			String clustersPath = job.get(CURRENT_PATH_CLUSTER_PROPERTY);
			try {
				FileSystem fs = FileSystem.get(new Path(clustersPath).toUri(),
						job);
				load(new Path(clustersPath), fs, job);
			} catch (IOException e) {
				throw new RuntimeException(e.getCause());
			}
		}

		void load(Path path, FileSystem fs, JobConf job) throws IOException {
			System.out.println("Loading Clusters: " + path.getName());
			FileStatus[] statuses = fs.listStatus(path, new PathFilter() {
				public boolean accept(Path path) {
					return path.toString().contains("part-");
				}
			});

			for (FileStatus status : statuses) {
				System.out.println(status.getPath().toString());

				InputStream stream = fs.open(status.getPath());

				BufferedReader wordReader = new BufferedReader(
						new InputStreamReader(stream));
				try {
					String line;
					while ((line = wordReader.readLine()) != null) {
						System.out.println("ReadLine: " + line);
						String[] l2 = line.split("\t");
						Vector vec = Vector.createVectorWithoutNr(l2[1]);
						vec.setNr(Long.parseLong(l2[0]));
						clusters.add(vec);
					}
				} finally {
					wordReader.close();
				}
			}

		}

	}

	/**
	 * Combiner agreguje wartosci srednich dla zadanych kluczy.
	 * Wykonuje operacje analogiczna do tej ktora wykonywana jest przez klase Reduce,
	 * z tym wyjatkiem ze zapisywana jest rowniez wartosc oznaczajaca krotnosc elementow w agregowanej sredniej.
	 * @author Maciej Mazur
	 *
	 */
	public static class Combiner extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter arg3)
				throws IOException {
			long count = 0;

			Vector sum = null;
			while (values.hasNext()) {
				Text t = values.next();
				Vector v = Vector.createVector(t.toString());
				v.multiply(v.getNr());
				if (sum == null) {
					sum = v;
				} else {
					sum.add(v);
				}
				count = count + v.getNr();
			}
			
			if (sum != null) {
				sum.setNr(count);
				sum.multiply(1 / (double) count);
				output.collect(key, new Text(sum.toString()));
			}

		}

	}

	/**
	 * Reducer
	 * Klucz - numer klastra.
	 * Wartosc - Wektor - ilosc elementow w sredniej i wektor reprezentujacy srednia
	 * Podczas funkcji reduce zostaja wczytane wektory reprezentujace srednie zaagregowanych wartosci
	 * przynalezacych do wybranego klastra. Glowna operacja, ktora zachodzi podczas operacji reduce jest
	 * agregowanie tych wartosci i wyliczanie sredniej ze wszystkich wartosci. Po dokonaniu tej operacji
	 * nastepuje zapisanie obliczonego usrednionego wektora, reprezuntujacego srodek klastra do pliku wyjsciowego.
	 * @author Maciej Mazur
	 *
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter arg3)
				throws IOException {
			long count = 0;
			Vector sum = null;
			
			while (values.hasNext()) {
				Text t = values.next();
				Vector v = Vector.createVector(t.toString());
				v.multiply(v.getNr());
				if (sum == null) {
					sum = v;
				} else {
					sum.add(v);
				}
				count = count + v.getNr();
			}

			if (sum != null) {
				sum.multiply(1 / (double) count);
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

	public ArrayList<Long> getErrors() {
		return errors;
	}


	
	public int getIt() {
		return it;
	}

	public void setIt(int it) {
		this.it = it;
	}

}
