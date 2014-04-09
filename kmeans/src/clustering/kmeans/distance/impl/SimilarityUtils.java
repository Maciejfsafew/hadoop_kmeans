package clustering.kmeans.distance.impl;

import clustering.kmeans.datatypes.Vector;
import clustering.kmeans.distance.DistanceMeasure;
/**
 * Prototype - don't touch
 * @author training
 *
 */
public class SimilarityUtils implements DistanceMeasure{

	public static final double EPS = 0.3; 
	public static boolean theSame(double a, double b){
		return Math.abs(a-b) < EPS;
	}
	/**
	 * ASCENDING ORDER
	 * Distance : 1/(number of repeted elements)
	 * [1,2,3,4] [1,2,3,4]   -> ~ 1/4
	 * [1,2,3,4] [0,1,2,4]   -> ~ 1/3
	 * [1,2,3,4] [1,3,5,7]   -> ~ 1/2
	 * [0,1,2,3,4] [5,6,7,8] -> ~ 1/0.00001
	 */
	@Override
	public double distance(Vector v1, Vector v2) {
		double d = 0.00001;
		double[] t1 = v1.getCoeficients();
		double[] t2 = v2.getCoeficients();
		
		int limit1 = t1.length;
		int limit2 = t2.length;
		
		int i = 0, j = 0;
		while(i<limit1 && j < limit2){
			if(theSame(t1[i],t2[j])){
				d = d + 1;
			}
			if(t1[i] < t2[j] ){
				i++;
			} else {
				j++;
			}
		}
		
		return 1/d;
	}
}
