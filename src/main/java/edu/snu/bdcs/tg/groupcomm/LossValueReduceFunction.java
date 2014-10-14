package edu.snu.bdcs.tg.groupcomm;

import javax.inject.Inject;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;

public class LossValueReduceFunction implements ReduceFunction<Double> {

  @Inject
  public LossValueReduceFunction() {

  }

  @Override
  public Double apply(Iterable<Double> values) {
    double sum = 0;
    int cnt = 0;
    for (Double val : values) {
      sum += val.doubleValue();
      cnt += 1;
    }
    
    return sum / cnt;
  }
  
}
