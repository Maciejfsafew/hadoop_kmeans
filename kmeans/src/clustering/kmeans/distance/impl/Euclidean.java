package clustering.kmeans.distance.impl;

import clustering.kmeans.datatypes.Vector;
import clustering.kmeans.distance.DistanceMeasure;

public class Euclidean implements DistanceMeasure{

	@Override
	public double distance(Vector v1, Vector v2) {
		double d = 0;
		double[] t1 = v1.getCoeficients();
		double[] t2 = v2.getCoeficients();
		
		int limit = Math.min(t1.length, t2.length);
		
		for (int i = 0; i < limit; i ++){
			d += Math.abs(t1[i]-t2[i]);
		}
		return d;
	}

}
