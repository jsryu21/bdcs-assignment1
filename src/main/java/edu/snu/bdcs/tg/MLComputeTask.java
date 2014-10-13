package edu.snu.bdcs.tg;

import javax.inject.Inject;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.task.Task;

import edu.snu.bdcs.tg.MLDriver.AllCommunicationGroup;
import edu.snu.bdcs.tg.groupcomm.SyncMessage;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorBroadcaster;
import edu.snu.bdcs.tg.groupcomm.operatornames.ParameterVectorReducer;
import edu.snu.bdcs.tg.groupcomm.operatornames.SyncMessageBroadcaster;

public class MLComputeTask implements Task {


  private CommunicationGroupClient communicationGroupClient;
  private Broadcast.Receiver<SyncMessage> syncMessageBroadcaster;
  private Broadcast.Receiver<Object> paramBroadcaster;
  private Reduce.Sender<Boolean> paramReducer;

  @Inject
  public MLComputeTask(final GroupCommClient groupCommClient) {
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.syncMessageBroadcaster = communicationGroupClient.getBroadcastReceiver(SyncMessageBroadcaster.class);
    this.paramBroadcaster = communicationGroupClient.getBroadcastReceiver(ParameterVectorBroadcaster.class);
    this.paramReducer = communicationGroupClient.getReduceSender(ParameterVectorReducer.class);
  }
  
  @Override
  public byte[] call(byte[] arg0) throws Exception {
    // TODO
    
    SyncMessage message = syncMessageBroadcaster.receive();
    
    System.out.println("ComputeTask receives start message: " + message);
    return null;
  }

}
