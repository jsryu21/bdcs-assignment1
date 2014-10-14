package edu.snu.bdcs.tg.vector;

import java.io.Serializable;

import javax.inject.Inject;

public class MLVector implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  private final double[] arr;
  
  @Inject
  public MLVector(double[] arr) {
    this.arr = arr;
  }
  
  public int getSize() {
    return arr.length;
  }
  
  public double get(int index) throws Exception {
    if (index < 0 && index >= arr.length) {
      throw new Exception("MLVector out of index: " + index + ", size: " + arr.length);
    }
    return arr[index];
  }
  
  public MLVector add(MLVector newVector) throws Exception {
    if (getSize() != newVector.getSize()) {
      throw new Exception("Vector addition should have same length. len a: " + getSize() + ", len b: " + newVector.getSize());
    }

    double[] c = new double[arr.length];
    
    for (int i = 0; i < arr.length; i++) {
      c[i] = arr[i] + newVector.get(i);
    }
    
    return new MLVector(c);
  }
  
  public MLVector minus(MLVector vec) throws Exception {
    return add(vec.scale(-1));
  }
  
  public MLVector scale(double a) {
    double[] c = new double[arr.length];
    
    for (int i = 0; i < arr.length; i++) {
      c[i] = arr[i] * a;
    }
    
    return new MLVector(c);
  }
  
  public double mult(MLVector vec) throws Exception {
    
    if (getSize() != vec.getSize()) {
      throw new Exception("Vector multiplication should have same length. len a: " + getSize() + ", len b: " + vec.getSize());
    }
    
    double sum = 0.0;
    int len = arr.length;
    for (int i = 0; i < len; i++) {
      sum += arr[i] * vec.get(i);
    }
    
    return sum;
  }
 
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    
    sb.append("[");
    
    for (int i = 0; i < arr.length; i++) {
      sb.append(String.format("%.2f ", arr[i]));
    }
    sb.append("]");
    
    return sb.toString();
  }
}
