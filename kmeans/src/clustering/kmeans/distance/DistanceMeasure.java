package clustering.kmeans.distance;

import clustering.kmeans.datatypes.Vector;

public interface DistanceMeasure {
	double distance(Vector v1, Vector v2);
}
