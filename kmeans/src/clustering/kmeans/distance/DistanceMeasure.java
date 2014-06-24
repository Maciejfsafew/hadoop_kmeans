package clustering.kmeans.distance;

import clustering.kmeans.datatypes.Vector;

/**
 * Interfejs deklarujacy mozliwosc ustalenia odleglosci dwoch wektorow.
 * @author Maciej Mazur
 *
 */
public interface DistanceMeasure {
	double distance(Vector v1, Vector v2);
}
