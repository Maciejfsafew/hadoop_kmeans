package clustering.kmeans.impl;

import java.util.ArrayList;

public class KMeans {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out
					.println("Usage: \n hadoop ... input_path output_path max_iterations");
			return;
		}
		String input = args[0];
		String output = args[1];
		String countOutput = output + "_count";
		String clustersInitialPath = output + "_0";
		
		/**
		 * Maksymalna liczba iteracji jest parametrem ograniczajacym. 
		 * Klasteryzacja moze zakonczyc sie przy mniejszej liczbie iteracji, zgodnie z regula lokcia.
		 */
		int iterations = Integer.parseInt(args[2]);

		ValueCounter counter = new ValueCounter();
		long records = counter.countValues(input, countOutput);
		long prevError = Long.MAX_VALUE;
		long currentError = 0;
		ArrayList<ArrayList<Long>> errors = new ArrayList<ArrayList<Long>>();
		ArrayList<Long> time = new ArrayList<Long>();
		ArrayList<Integer> iters = new ArrayList<Integer>();
		ArrayList<Long> errs = new ArrayList<Long>();
		System.out.println("########################################################### COUNTED RECORDS: " + records);
		long totalStart = System.currentTimeMillis();
		for(int clusters = 2; clusters < iterations; clusters++){
			long begin = System.currentTimeMillis();
			ArrayList<Long> localErrors = new ArrayList<Long>();
			currentError = Long.MAX_VALUE;
			for(int k = 0; k < 3; k++){
				ClustersRandomInitialization initialization = new ClustersRandomInitialization(
					clusters, records, input, clustersInitialPath+"_"+clusters+"_"+k);
	
				int initRandom = initialization.execute();
				if(initRandom!=0){
					throw new Exception("########################################################### Errior during initialization");
				}
	
				Clustering clustering = new Clustering(input, output+clusters+"_"+k, clustersInitialPath+"_"+clusters+"_"+k,iterations);
			
				clustering.run();
				
				ArrayList<Long> helpErrors = clustering.getErrors();
				long helpError = helpErrors.get(helpErrors.size()-1);
				if(currentError>helpError){
					currentError = helpError;
					localErrors = helpErrors;
				}
				
			}

			errs.add(currentError);
			errors.add(localErrors);
			iters.add(localErrors.size());
			time.add(System.currentTimeMillis()-begin);
			System.out.println("########################################################### CURRENT ERROR: " + currentError+" CLUSTERS : "+clusters );
			if(Math.abs(prevError - currentError) < 0.1 * prevError){
				break;
			}
			prevError = currentError;

			
		}
		System.out.println("Errors:");
		for(int i = 0; i < errors.size(); i ++){
			StringBuilder sb = new StringBuilder();
			sb.append("clusters: "+(2+i)+" err: (" +errs.get(i)+") ");
			for(Long l : errors.get(i))
				sb.append(l+" ");
			sb.append(" time: "+time.get(i));
			sb.append(" iterations: "+iters.get(i));
			System.out.println(sb.toString());
		}
		
		System.out.println("Total clustering time: "+(System.currentTimeMillis()-totalStart));
		
	}
}