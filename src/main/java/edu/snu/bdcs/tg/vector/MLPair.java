package edu.snu.bdcs.tg.vector;

public class MLPair {
  private final MLVector vector;
  private final int value;
  
  public MLPair(MLVector vector, int value) {
    this.vector = vector;
    this.value = value;
  }
  
  public MLVector getX() {
    return vector;
  }
  
  public int getY() {
    return value;
  }
}