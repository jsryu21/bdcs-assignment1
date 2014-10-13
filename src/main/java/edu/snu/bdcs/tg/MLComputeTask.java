package edu.snu.bdcs.tg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.inject.Inject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.io.network.util.Pair;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

import edu.snu.bdcs.tg.MLAssignment.LearningRate;
import edu.snu.bdcs.tg.MLAssignment.NumFeatures;
import edu.snu.bdcs.tg.MLAssignment.NumIterations;
import edu.snu.bdcs.tg.MLDriver.AllCommunicationGroup;
import edu.snu.bdcs.tg.groupcomm.SyncMessage;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorBroadcaster;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorReducer;
import edu.snu.bdcs.tg.groupcomm.operatornames.SyncMessageBroadcaster;
import edu.snu.bdcs.tg.vector.MLVector;

public class MLComputeTask implements Task {


  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Receiver<SyncMessage> syncMessageBroadcaster;
  private final Broadcast.Receiver<MLVector> paramBroadcaster;
  private final Reduce.Sender<MLVector> paramReducer;
  private final DataSet<LongWritable, Text> dataSet;
  private final int numFeatures;
  private MLVector parameters;
  private final double learningRate;
  private final int iterations;
  private final String identifier;
  
  @Inject
  public MLComputeTask(final GroupCommClient groupCommClient,
      final DataSet<LongWritable, Text> dataSet, 
      final @Parameter(NumFeatures.class) int numFeatures, 
      final @Parameter(LearningRate.class) double learningRate, 
      final @Parameter(NumIterations.class) int iterations, 
      final @Parameter(TaskConfigurationOptions.Identifier.class) String identifier) {
    
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.syncMessageBroadcaster = communicationGroupClient.getBroadcastReceiver(SyncMessageBroadcaster.class);
    this.paramBroadcaster = communicationGroupClient.getBroadcastReceiver(ParameterVectorBroadcaster.class);
    this.paramReducer = communicationGroupClient.getReduceSender(ParameterVectorReducer.class);
    this.dataSet = dataSet;
    this.numFeatures = numFeatures;
    this.learningRate = learningRate;
    this.iterations = iterations;
    this.identifier = identifier;
  }
  
  @Override
  public byte[] call(byte[] arg0) throws Exception {
    
    //SyncMessage message = syncMessageBroadcaster.receive();
    //System.out.println("ComputeTask receives start message: " + message);
    
    List<MLPair> trainingList = new ArrayList<>(100);

    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      // key is byte number
      String value = keyValue.second.toString();
      String[] splited = value.split("\t");

      MLVector xArr = getFeatureVector(splited);
      int Y = Integer.valueOf(splited[splited.length - 1]).intValue();
      trainingList.add(new MLPair(xArr, Y));
    }

    
    for (int i = 0; i < iterations; i++) {
      // shuffle 
      long seed = System.nanoTime();
      Collections.shuffle(trainingList, new Random(seed));
      
      // get averaged parameter values
      parameters = paramBroadcaster.receive();

      System.out.println(identifier + " Iteration " + i + " start...");
      for (final MLPair training : trainingList) {
        parameters = updateParameters(parameters, training.vector, training.value, learningRate);
      }
      
      System.out.println(identifier + " Iteration " + i + " end...");
      System.out.println("Sending parameter: " + parameters);
      paramReducer.send(parameters);
    }

    return null;
  }
  

  
  private MLVector getFeatureVector(String[] strs) {

    // last value is Y
    double[] dArr = new double[strs.length];
    dArr[0] = 1; 
    for (int i = 1; i < strs.length; i++) {
      dArr[i] = Integer.valueOf(strs[i-1]).intValue();
    }
    
    return new MLVector(dArr);
  }
  
  private MLVector updateParameters(MLVector parameters, MLVector xArr, int Y, double learningRate) throws Exception {
    return parameters.add(gradientLossFunction(Y, xArr, parameters).scale(-learningRate));
  }
  
  private MLVector gradientLossFunction(int Y, MLVector X, MLVector params) throws Exception {
    double Z = params.mult(X);
    
    if (Z >=1 || Z <= 1) {
      return X.add(params);
    } else {
      return params;
    }
  }
  
  private class MLPair {
    private final MLVector vector;
    private final int value;
    
    public MLPair(MLVector vector, int value) {
      this.vector = vector;
      this.value = value;
    }
  }
}
