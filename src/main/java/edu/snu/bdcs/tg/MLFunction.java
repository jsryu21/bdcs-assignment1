package edu.snu.bdcs.tg;

import java.util.List;

import edu.snu.bdcs.tg.vector.MLPair;
import edu.snu.bdcs.tg.vector.MLVector;

public class MLFunction {

  public static double getLoss(List<MLPair> trainingSet, MLVector params, double lambda) throws Exception {
    
    double cost = 0;
    
    for (MLPair tuple : trainingSet) {
      cost += lossFunction(tuple.getX(), params, tuple.getY());
    }
    
    return lambda * 0.5 * params.mult(params) + cost;
  }
  
  public static double lossFunction(MLVector X, MLVector params, int Y) throws Exception {
    return Math.max(0, 1 - Y * params.mult(X));
  }
  
  public static MLVector getSGD(List<MLPair> trainingSet, MLVector params, double learningRate, double lambda) throws Exception {
        
    for (MLPair tuple : trainingSet) {
      MLVector X = tuple.getX();
      int Y = tuple.getY();
      
      double Z = Y * params.mult(X);
      
      if (Z < 1) {
        MLVector v = params.scale(lambda).minus(X.scale(Y));
        params = params.minus(v.scale(learningRate));
      } else {
        params = params.minus(params.scale(lambda * learningRate));
      }
    }
    
    return params;
  }
}
