package edu.snu.bdcs.tg;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.task.Task;

import edu.snu.bdcs.tg.groupcomm.SyncMessage;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorBroadcaster;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorReducer;
import edu.snu.bdcs.tg.groupcomm.operatornames.SyncMessageBroadcaster;

public class MLControllerTask implements Task {

  public static final String TASK_ID = "MLControllerTask";
  private CommunicationGroupClient communicationGroupClient;
  private Broadcast.Sender<SyncMessage> syncMessageBroadcaster;
  private Broadcast.Sender<Object> paramBroadcaster;
  private Reduce.Receiver<Boolean> paramReducer;
  
  @Inject
  public MLControllerTask(final GroupCommClient groupCommClient) {
    
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.syncMessageBroadcaster = communicationGroupClient.getBroadcastSender(SyncMessageBroadcaster.class);
    this.paramBroadcaster = communicationGroupClient.getBroadcastSender(ParameterVectorBroadcaster.class);
    this.paramReducer = communicationGroupClient.getReduceReceiver(ParameterVectorReducer.class);
  }

  @Override
  public byte[] call(byte[] arg0) throws Exception {
    
    syncMessageBroadcaster.send(SyncMessage.Start);
    System.out.println("ControllerTask sends start message");
    return null;
  }
  
  
}
