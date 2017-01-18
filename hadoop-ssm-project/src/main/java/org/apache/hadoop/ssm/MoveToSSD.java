package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by cc on 17-1-15.
 */
public  class MoveToSSD extends ActionBase{
  String fileName;
//  static MoveToSSD instance;
  private DFSClient dfsClient;
  Configuration conf;
  private LinkedBlockingQueue<MoverExecutor.FileAction> actionEvents;



  public MoveToSSD(DFSClient client, Configuration conf, String fileName) {
    super(client);
    this.dfsClient=client;
    this.conf=conf;
    this.fileName=fileName;
  }


//  public MoveToSSD getInstance(DFSClient dfsClient,Configuration conf,String fileName,ActionType actionType) {
//    if (instance == null) {
//      instance = new MoveToSSD(dfsClient, conf,fileName);
//    }
//    return instance;
//  }

  public  void initial(String[] args){

  }

  /**
   * Execute an action.
   * @return true if success, otherwise return false.
   */
  public boolean execute(){
    Action action =Action.getActionType("ssd");
    // suo
    MoverExecutor.getInstance(dfsClient,conf).addActionEvent(fileName,action);
    MoverExecutor.getInstance(dfsClient,conf).run();
    return true;
  }

}
