package edu.snu.bdcs.tg.groupcomm;

import javax.inject.Inject;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;

import edu.snu.bdcs.tg.vector.MLVector;

public class ParameterVectorReduceFunction implements ReduceFunction<MLVector> {

  @Inject
  ParameterVectorReduceFunction() {
    
  }
  
  public MLVector apply(Iterable<MLVector> vectors) {
    // TODO
    
    MLVector result = null;
    int i = 0;
    for (MLVector vector : vectors) {
      if (result == null) {
        result = vector;
      } else {
        try {
          result.add(vector);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      
      i += 1;
    }
    
    return result.scale(1.0 / i);
  }

}
