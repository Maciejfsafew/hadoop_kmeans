package clustering.kmeans.datatypes;

import java.util.Arrays;

public class Vector {
	

	long nr;
	
	/**
	 * Ascending order
	 */
	double coeficients[];
	
	private Vector(long nr, double t[]){
		this.nr = nr;
		this.coeficients = t;
	}
	
	public static Vector createVector(String s){
		if(s!= null){
			String t[] = s.split(" ");
			if(t.length>2){
				double [] vals = new double[t.length-1];

				for(int i = 1; i < t.length; i++){
					vals[i-1] = Double.parseDouble(t[i]);
				}
				return new Vector(Long.parseLong(t[0]),vals);
			}
		}
		
		return null;
	}
	
	
	public static Vector createVector(int nr, double t[]){
		if(t!= null){
			return new Vector(nr,Arrays.copyOf(t, t.length));
		}
		return null;
	}

	public long getNr() {
		return nr;
	}

	public void setNr(long nr) {
		this.nr = nr;
	}

	public double[] getCoeficients() {
		return coeficients;
	}

	public void setCoeficients(double[] coeficients) {
		this.coeficients = coeficients;
	}
	
	
	
}
