package edu.snu.bdcs.tg;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

import edu.snu.bdcs.tg.MLLauncher.NumFeatures;
import edu.snu.bdcs.tg.MLLauncher.NumIterations;
import edu.snu.bdcs.tg.MLDriver.AllCommunicationGroup;
import edu.snu.bdcs.tg.groupcomm.operatornames.LossValueReducer;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorBroadcaster;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorReducer;
import edu.snu.bdcs.tg.vector.MLVector;

public class MLAggregateTask implements Task {
  private static final Logger LOG = Logger.getLogger(MLAggregateTask.class.getName());

  public static final String TASK_ID = "MLAggregateTask";
  private CommunicationGroupClient communicationGroupClient;
  private Broadcast.Sender<MLVector> paramBroadcaster;
  private Reduce.Receiver<MLVector> paramReducer;
  private Reduce.Receiver<Double> lossReducer;
  
  private MLVector parameters;
  private final int iterations;
  private final String identifier;

  @Inject
  public MLAggregateTask(final GroupCommClient groupCommClient, 
      final @Parameter(NumFeatures.class) int numFeatures,
      final @Parameter(NumIterations.class) int iterations, 
      final @Parameter(TaskConfigurationOptions.Identifier.class) String identifier) {
    
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.paramBroadcaster = communicationGroupClient.getBroadcastSender(ParameterVectorBroadcaster.class);
    this.paramReducer = communicationGroupClient.getReduceReceiver(ParameterVectorReducer.class);
    this.lossReducer = communicationGroupClient.getReduceReceiver(LossValueReducer.class);
    
    this.iterations = iterations;
    this.identifier = identifier;
    
    double[] params = new double[numFeatures + 1];
    initializeParameter(params); // Initialize to 0
    parameters = new MLVector(params);
  }

  @Override
  public byte[] call(byte[] arg0) throws Exception {
    
    StringBuilder sb = new StringBuilder();
    
    
    sb.append("Iteration 0 Parameters : " + parameters).append("\n");
    for ( int i = 0; i < iterations; i++) {
      LOG.log(Level.INFO, identifier + " Iteration " + i + " start...");
      
      paramBroadcaster.send(parameters);
      // aggregate loss value
      double loss = lossReducer.reduce();
      sb.append("Iteration " + i + " loss: " + String.format("%.2f", loss)).append("\n");
      parameters = paramReducer.reduce();
      sb.append("Iteration " + (i + 1) + " Parameters : " + parameters).append("\n");
    }
    
    double loss = lossReducer.reduce();
    sb.append("Iteration " + iterations + " loss: " + String.format("%.2f", loss)).append("\n");
    
    return sb.toString().getBytes();
  }
  
  private void initializeParameter(double[] parameters) {
    for (int i = 0; i < parameters.length; i++) {
      parameters[0] = 0;
    }
  }
}
