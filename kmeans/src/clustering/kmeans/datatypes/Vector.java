package clustering.kmeans.datatypes;

import java.util.Arrays;

public class Vector {
	

	long nr;
	
	double coeficients[];
	
	public Vector(int size){
		coeficients = new double[size];
	}
	
	public void add(Vector v){
		if(v==null||v.getCoeficients()==null||v.getCoeficients().length!=coeficients.length){
			System.out.println("Add Error, wrong vector");
			return;
		}
		double coefs[] = v.getCoeficients();
		for(int i = 0; i < coeficients.length; i ++){
			coeficients[i] += coefs[i];
		}	
	}
	
	public void multiply(double v){
		for(int i = 0; i < coeficients.length; i ++){
			coeficients[i] *= v;
		}
	}
	
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
	
	public static Vector createVectorWithoutNr(String s){
		if(s!= null){
			String t[] = s.split(" ");
			if(t.length>1){
				double [] vals = new double[t.length];

				for(int i = 0; i < t.length; i++){
					vals[i] = Double.parseDouble(t[i]);
				}
				return new Vector(0,vals);
			}
		}
		
		return null;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(nr);
		for(int i = 0; i < coeficients.length; i++){
			sb.append(" ");
			sb.append(coeficients[i]);
		}
		
		return sb.toString();
	}
	
	public String toStringWithoutNr() {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < coeficients.length; i++){
			if(i != 0){
				sb.append(" ");
			}
			sb.append(coeficients[i]);
		}
		return sb.toString();
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
