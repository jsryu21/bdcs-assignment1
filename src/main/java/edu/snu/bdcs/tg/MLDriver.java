/**
   * Copyright (C) 2014 Microsoft Corporation
   *
   * Licensed under the Apache License, Version 2.0 (the "License");
   * you may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *         http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */

package edu.snu.bdcs.tg;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.network.nggroup.api.driver.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.driver.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;

import edu.snu.bdcs.tg.groupcomm.ParameterVectorReduceFunction;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorBroadcaster;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorReducer;
import edu.snu.bdcs.tg.groupcomm.operatornames.SyncMessageBroadcaster;

/**
 * Driver side for the line counting demo that uses the data loading service.
 */
@DriverSide
@Unit
public class MLDriver {

  @NamedParameter()
  public static final class AllCommunicationGroup implements Name<String> {}

  private static final Logger LOG = Logger.getLogger(MLDriver.class.getName());

  private final AtomicInteger computeIds = new AtomicInteger();
  private final AtomicInteger lineCnt = new AtomicInteger();
  private final AtomicInteger completedDataTasks = new AtomicInteger();
  private final AtomicInteger numberOfAllocatedEvaluators;
  private final AtomicBoolean aggregatorSubmitted = new AtomicBoolean(false);

  private final int numOfComputeTasks;
  
  private final DataLoadingService dataLoadingService;
  private final GroupCommDriver groupCommDriver;
  private final CommunicationGroupDriver allCommGroup;
  private final ConfigurationSerializer confSerializer;
  
  private String groupCommConfiguredMasterId;


  @Inject
  public MLDriver(final DataLoadingService dataLoadingService, 
      final GroupCommDriver groupCommDriver, 
      final ConfigurationSerializer confSerializer) {
    this.dataLoadingService = dataLoadingService;
    this.numOfComputeTasks = dataLoadingService.getNumberOfPartitions();
    this.completedDataTasks.set(numOfComputeTasks);
    this.groupCommDriver = groupCommDriver;
    this.confSerializer = confSerializer;

    this.numberOfAllocatedEvaluators = new AtomicInteger(numOfComputeTasks + 1);
    this.allCommGroup = this.groupCommDriver.newCommunicationGroup(
        AllCommunicationGroup.class, numOfComputeTasks + 1);
    
    this.allCommGroup
    .addBroadcast(SyncMessageBroadcaster.class,
        BroadcastOperatorSpec.newBuilder()
            .setSenderId(MLAggregateTask.TASK_ID)
            .setDataCodecClass(SerializableCodec.class)
            .build())
    .addBroadcast(ParameterVectorBroadcaster.class,
        BroadcastOperatorSpec.newBuilder()
            .setSenderId(MLAggregateTask.TASK_ID)
            .setDataCodecClass(SerializableCodec.class)
            .build())
    .addReduce(ParameterVectorReducer.class,
        ReduceOperatorSpec.newBuilder()
            .setReceiverId(MLAggregateTask.TASK_ID)
            .setDataCodecClass(SerializableCodec.class)
            .setReduceFunctionClass(ParameterVectorReduceFunction.class)
            .build())
    .finalise();
  }

  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    private final AtomicBoolean storeMasterId = new AtomicBoolean(false);

    @Override
    public void onNext(final ActiveContext activeContext) {

      final String contextId = activeContext.getId();
      LOG.log(Level.FINER, "Context active: {0}", contextId);

      if (dataLoadingService.isDataLoadedContext(activeContext)) {
        // submit compute context and service

        final Configuration contextConf = groupCommDriver.getContextConfiguration();
        final Configuration serviceConf = groupCommDriver.getServiceConfiguration();
        LOG.log(Level.FINER, "Submit GC compute conf: {0}", confSerializer.toString(contextConf));
        LOG.log(Level.FINER, "Submit Service conf: {0}", confSerializer.toString(serviceConf));

        activeContext.submitContextAndService(contextConf, serviceConf);

      } else if (groupCommDriver.isConfigured(activeContext)) {
        
        if (activeContext.getId().equals(groupCommConfiguredMasterId) && !aggregatorTaskSubmitted()) {

          final Configuration partialTaskConf = Tang.Factory.getTang()
              .newConfigurationBuilder(
                  TaskConfiguration.CONF
                      .set(TaskConfiguration.IDENTIFIER, MLAggregateTask.TASK_ID)
                      .set(TaskConfiguration.TASK, MLAggregateTask.class)
                      .build())
              .build();

          allCommGroup.addTask(partialTaskConf);

          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.log(Level.FINER, "Submit aggregatorTask conf: {0}", confSerializer.toString(taskConf));

          activeContext.submitTask(taskConf);

        } else {

          final Configuration partialTaskConf = Tang.Factory.getTang()
              .newConfigurationBuilder(
                  TaskConfiguration.CONF
                      .set(TaskConfiguration.IDENTIFIER, getComputeId(activeContext))
                      .set(TaskConfiguration.TASK, MLComputeTask.class)
                      .build())
              .build();

          allCommGroup.addTask(partialTaskConf);

          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.log(Level.FINER, "Submit ComputeTask conf: {0}", confSerializer.toString(taskConf));

          activeContext.submitTask(taskConf);
        }
        
      } else {
        // submit aggregator context and service
        final Configuration contextConf = groupCommDriver.getContextConfiguration();
        final String groupCommContextId = contextId(contextConf);

        if (storeMasterId.compareAndSet(false, true)) {
          groupCommConfiguredMasterId = groupCommContextId;
        }

        final Configuration serviceConf = groupCommDriver.getServiceConfiguration();
        LOG.log(Level.FINER, "Submit GC aggregator conf: {0}", confSerializer.toString(contextConf));
        LOG.log(Level.FINER, "Submit Service conf: {0}", confSerializer.toString(serviceConf));

        activeContext.submitContextAndService(contextConf, serviceConf);
      }
    }
  }
  
  private String contextId(final Configuration contextConf) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
      return injector.getNamedInstance(ContextIdentifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to inject context identifier from context conf", e);
    }
  }
  
  private String getComputeId(final ActiveContext activeContext) {
    return "MLComputeTask-" + computeIds.getAndIncrement();
  }
  
  private boolean aggregatorTaskSubmitted() {
    return !aggregatorSubmitted.compareAndSet(false, true);
  }

  public class TaskCompletedHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {
      
      final String taskId = completedTask.getId();
      LOG.log(Level.FINEST, "Releasing Context: {0}", taskId);
      completedTask.getActiveContext().close();
      
      /*
      final String taskId = completedTask.getId();
      LOG.log(Level.FINEST, "Completed Task: {0}", taskId);

      final byte[] retBytes = completedTask.get();
      final String retStr = retBytes == null ? "No RetVal": new String(retBytes);
      LOG.log(Level.FINE, "Line count from {0} : {1}", new String[] { taskId, retStr });

      lineCnt.addAndGet(Integer.parseInt(retStr));

      if (completedDataTasks.decrementAndGet() <= 0) {
        LOG.log(Level.INFO, "Total line count: {0}", lineCnt.get());
      }

      LOG.log(Level.FINEST, "Releasing Context: {0}", taskId);
      completedTask.getActiveContext().close();
      
      */
    }
  }
}


