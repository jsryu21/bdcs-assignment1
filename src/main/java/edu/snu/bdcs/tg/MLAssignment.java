package edu.snu.bdcs.tg;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.mapred.TextInputFormat;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.io.network.nggroup.impl.driver.GroupCommService;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;

/*
 * Main class of assignment 
 * Setup driver for ml 
 * 
 */
public class MLAssignment {
  private static final Logger LOG = Logger.getLogger(MLAssignment.class.getName());

  private static final int NUM_LOCAL_THREADS = 16;
  private static final int NUM_SPLITS = 4;
  private static final int NUM_COMPUTE_EVALUATORS = 1;
  private static final int MEM_COMPUTE_EVALUATORS = 256;
  private static final int MEM_DATALOADING_EVALUATORS = 512;

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime", short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
    
  }

  @NamedParameter(doc = "Number of minutes before timeout", short_name = "timeout", default_value = "10")
  public static final class TimeOut implements Name<Integer> {
    
  }

  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
    
  }
  
  @NamedParameter(short_name = "num_features", default_value = "3")
  public static final class NumFeatures implements Name<Integer> {

  }
  
  @NamedParameter(short_name = "learning_rate", default_value = "0.01")
  public static final class LearningRate implements Name<Double> {

  }
  
  @NamedParameter(short_name = "lambda", default_value = "0.001")
  public static final class Lambda implements Name<Double> {

  }
  
  @NamedParameter(short_name = "num_iterations", default_value = "10")
  public static final class NumIterations implements Name<Integer> {

  }
  
  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {

    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    CommandLine cl = new CommandLine(cb)
    .registerShortNameOfClass(Local.class)
    .registerShortNameOfClass(TimeOut.class)
    .registerShortNameOfClass(MLAssignment.InputDir.class)
    .registerShortNameOfClass(NumFeatures.class)
    .registerShortNameOfClass(LearningRate.class)
    .registerShortNameOfClass(Lambda.class)
    .registerShortNameOfClass(NumIterations.class)
    .processCommandLine(args);

    return cl.getBuilder().build();
  }

  private static Configuration getDataLoadingConf(String inputDir) {

    // This request is for controller task.
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

    return dataLoadConfiguration;
  }

  private static Configuration getRuntimeConfiguration(boolean isLocal) {
    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running MLAssignment on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running MLAssignment on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    return runtimeConfiguration;
  }

  public static void main(final String[] args)
      throws InjectionException, BindException, IOException {

    Configuration commandConf = getCommandLineConf(args);

    final Tang tang = Tang.Factory.getTang();
    final Injector injector = tang.newInjector(commandConf);

    final boolean isLocal = injector.getNamedInstance(Local.class);
    final int jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    final String inputDir = injector.getNamedInstance(MLAssignment.InputDir.class);


    final Configuration runtimeConfiguration = getRuntimeConfiguration(isLocal);
    final Configuration dataLoadingConfiguration = getDataLoadingConf(inputDir);
    final Configuration groupCommServConfiguration = GroupCommService.getConfiguration();

    final Configuration mergedConf = Tang.Factory.getTang().newConfigurationBuilder(
        commandConf, 
        dataLoadingConfiguration,
        groupCommServConfiguration)
        .build();

    final LauncherStatus state =
        DriverLauncher.getLauncher(runtimeConfiguration).run(mergedConf, jobTimeout);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }
}
