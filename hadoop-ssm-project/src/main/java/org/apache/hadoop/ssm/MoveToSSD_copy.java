package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.FilesInfo;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.hdfs.protocol.FilesInfo.STORAGEPOLICY;

/**
 * Created by cc on 17-1-15.
 */
public  class MoveToSSD_copy extends ActionBase {

  private static MoveToSSD_copy instance;
  public static final byte ALLSSD_STORAGE_POLICY_ID = 12;
  String fileName;
  private DFSClient dfsClient;
  Configuration conf;
//  boolean flag=false;
//  private LinkedBlockingQueue<FileAction> actionEvents;

  class FileAction {
    String fileName;
    Action action;

    FileAction(String fileName, Action action) {
      this.fileName = fileName;
      this.action = action;
    }
  }

  public MoveToSSD_copy(DFSClient client, Configuration conf) {
    super(client);
    this.dfsClient = client;
    this.conf = conf;
//    this.actionEvents = new LinkedBlockingQueue<>();
  }

  public static synchronized MoveToSSD_copy getInstance(DFSClient dfsClient, Configuration conf) {
    if (instance == null) {
      instance = new MoveToSSD_copy(dfsClient, conf);
    }
    return instance;
  }

  public void initial(String[] args) {
    this.fileName = args[0];
  }

  /**
   * Execute an action.
   *
   * @return true if success, otherwise return false.
   */
  public boolean execute() {
//    Action action = Action.getActionType("ssd");
    // suo
//    instance.addActionEvent(fileName, action);
    if(runSSD(fileName)){
      return true;
      }else{
      return false;
    }
  }

  private boolean runSSD(String fileName) {
    if (getStoragePolicy(fileName) == ALLSSD_STORAGE_POLICY_ID) {
      return true;
    }
    System.out.println("*" + System.currentTimeMillis() + " : " + fileName + " -> " + "ssd");
    try {
      dfsClient.setStoragePolicy(fileName, "ALL_SSD");
    } catch (Exception e) {
      return false;
    }
    try {
      ToolRunner.run(conf, new Mover.Cli(),
              new String[]{"-p", fileName});
    } catch (Exception e) {
      return false;
    }
    return true;
  }


//  public void addActionEvent(String fileName, Action action) {
//    try {
//      actionEvents.put(new MoveToSSD_copy.FileAction(fileName, action));
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }


  private byte getStoragePolicy(String fileName) {
    FilesInfo filesInfo;
    String[] paths = {fileName};

    try {
      filesInfo = dfsClient.getFilesInfo(paths, STORAGEPOLICY, false, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    byte storagePolicy = filesInfo.getStoragePolicy().get(0);
    return storagePolicy;
  }

}



