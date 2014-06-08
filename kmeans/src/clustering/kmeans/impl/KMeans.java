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
		int iterations = Integer.parseInt(args[2]);

		ValueCounter counter = new ValueCounter();
		long records = counter.countValues(input, countOutput);
		long prevError = Long.MAX_VALUE;
		long currentError = 0;
		ArrayList<Long> errors = new ArrayList<Long>();
		ArrayList<Long> time = new ArrayList<Long>();
		System.out.println("########################################################### COUNTED RECORDS: " + records);
		
		for(int clusters = 2; clusters < 400; clusters *=2){
			long begin = System.currentTimeMillis();
			ClustersRandomInitialization initialization = new ClustersRandomInitialization(
					clusters, records, input, clustersInitialPath+"_"+clusters);
	
			int initRandom = initialization.execute();
			if(initRandom!=0){
				throw new Exception("########################################################### Errior during initialization");
			}
	
			Clustering clustering = new Clustering(input, output+clusters, clustersInitialPath+"_"+clusters,iterations);
			
			clustering.run();
			currentError = clustering.getError();
			errors.add(currentError);
			System.out.println("########################################################### CURRENT ERROR: " + currentError+" CLUSTERS : "+clusters );
			if(Math.abs(prevError - currentError) < 0.1 * prevError){
				break;
			}
			time.add(System.currentTimeMillis());
		}
		System.out.println("Errors:");
		for(int i = 0; i < errors.size(); i ++){
			System.out.println("err: "+errors.get(i)+" time: "+time.get(i));
		}
		
	}
}