package edu.snu.bdcs.tg;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.client.ClientConfiguration;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;

/*
 * Main class of assignment 
 * Setup driver for ml 
 * 
 */
public class MLLauncher {
  private static final Logger LOG = Logger.getLogger(MLLauncher.class.getName());

  private static final int NUM_LOCAL_THREADS = 16;


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
    .registerShortNameOfClass(InputDir.class)
    .registerShortNameOfClass(NumFeatures.class)
    .registerShortNameOfClass(LearningRate.class)
    .registerShortNameOfClass(Lambda.class)
    .registerShortNameOfClass(NumIterations.class)
    .processCommandLine(args);

    return cl.getBuilder().build();
  }
  

  private static Configuration getClientConfiguration(final String[] args)
      throws BindException, InjectionException, IOException {

    final Configuration commandLineConf = getCommandLineConf(args);

    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_MESSAGE, MLClient.JobMessageHandler.class)
        .build();

    // TODO: Remove the injector, have stuff injected.
    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);
    final boolean isLocal = commandLineInjector.getNamedInstance(Local.class);
    final Configuration runtimeConfiguration;
    
    if (isLocal) {
      LOG.log(Level.INFO, "Running on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    return Tang.Factory.getTang()
        .newConfigurationBuilder(runtimeConfiguration, clientConfiguration,
            commandLineConf)
        .build();
  }

  public static void main(final String[] args)
      throws InjectionException, BindException, IOException, InterruptedException {

    final Configuration clientConfiguration = getClientConfiguration(args);
    final Injector injector = Tang.Factory.getTang().newInjector(clientConfiguration);
    final MLClient client = injector.getInstance(MLClient.class);
    
    client.submit();
    client.waitForCompletion();
    LOG.log(Level.INFO, "REEF job completed");
  }
  

}
