package edu.snu.bdcs.tg;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.hadoop.mapred.TextInputFormat;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.JobMessage;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.io.network.nggroup.impl.driver.GroupCommService;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;

@Unit
public class MLClient {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(MLClient.class.getName());

  private static final int NUM_SPLITS = 4;
  private static final int NUM_COMPUTE_EVALUATORS = 1;
  private static final int MEM_COMPUTE_EVALUATORS = 256;
  private static final int MEM_DATALOADING_EVALUATORS = 512;
  
  private final CountDownLatch signal = new CountDownLatch(1);
  private final REEF reef;
  private final Configuration driverConf;
  
  @Inject
  public MLClient(final REEF reef, 
      @Parameter(MLLauncher.InputDir.class) String inputDir,
      @Parameter(MLLauncher.Lambda.class) double lambda,
      @Parameter(MLLauncher.LearningRate.class) double learningRate,
      @Parameter(MLLauncher.NumIterations.class) int iterations
      ) throws BindException {
    
    this.reef = reef;
    
    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(NUM_COMPUTE_EVALUATORS)
        .setMemory(MEM_COMPUTE_EVALUATORS)
        .build();

    // DataLoading request
    final Configuration dataLoadConfiguration = new DataLoadingRequestBuilder()
    .setMemoryMB(MEM_DATALOADING_EVALUATORS)
    .setInputFormatClass(TextInputFormat.class)
    .setInputPath(inputDir)
    .setNumberOfDesiredSplits(NUM_SPLITS)
    .setComputeRequest(computeRequest)
    .setDriverConfigurationModule(DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MLDriver.class))
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, MLDriver.ContextActiveHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, MLDriver.TaskCompletedHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "MLAssignment"))
        .build();
    
    final Configuration groupCommServConfiguration = GroupCommService.getConfiguration();

    driverConf = Tang.Factory.getTang().newConfigurationBuilder(
        dataLoadConfiguration,
        groupCommServConfiguration)
        .bindNamedParameter(MLLauncher.InputDir.class, inputDir)
        .bindNamedParameter(MLLauncher.Lambda.class, lambda+"")
        .bindNamedParameter(MLLauncher.LearningRate.class, learningRate+"")
        .bindNamedParameter(MLLauncher.NumIterations.class, iterations+"")
        .build();
  }

  /**
   * Receive message from the job driver.
   * There is only one message, which comes at the end of the driver execution
   * and contains shell command output on each node.
   */
  final class JobMessageHandler implements EventHandler<JobMessage> {
    @Override
    public void onNext(final JobMessage message) {

      String result;
      try {
        result = new String(message.get(), "UTF-8");
        signal.countDown();
        System.out.println("================== Result ===============");
        System.out.println(result);
        System.out.println("=========================================");
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
      
    }
  }
  
  public void submit() throws BindException, InjectionException {
    this.reef.submit(driverConf);
  }
  
  public void waitForCompletion() throws InterruptedException {
    this.signal.await();
    this.reef.close();
    LOG.log(Level.INFO, "End of client");
  }
}
