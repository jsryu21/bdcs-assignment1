package edu.snu.bdcs.tg.groupcomm;

import javax.inject.Inject;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;

public class ParameterVectorReduceFunction implements ReduceFunction<Boolean> {

  @Inject
  ParameterVectorReduceFunction() {
    
  }
  
  public Boolean apply(Iterable<Boolean> arg0) {
    // TODO
    return true;
  }

}
