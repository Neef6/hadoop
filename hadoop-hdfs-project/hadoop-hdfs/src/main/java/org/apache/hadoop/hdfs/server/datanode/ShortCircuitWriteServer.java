package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Time;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ShortCircuitWriteServer implements Runnable {
  public static final Log LOG = DataNode.LOG;

  private DataNode dataNode;
  private Configuration config;
  private int per_buffer_size ;
  private volatile boolean shutdown = false;

  private static final String BLOCK_TMP_DIR = "scwtemp";
  private static final byte[] META_DATA = new byte[]{0, 1, 0, 0, 0, 2, 0};

  private String blockPoolID;
  private List<? extends FsVolumeSpi> volumes;
  int nDirs;
  File[] finalizedDirs = null;
  String[] baseDirs = null;
  String[] blockTempDirs = null;
  String[] storageIDs = null;

  private int blockIndex = 0;

  private final long blockSize;

  ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

  ShortCircuitWriteServer(DataNode dataNode, Configuration config) {
    this.dataNode = dataNode;
    this.config = config;
    assert null != config;
    per_buffer_size = config.getInt("dfs.client.localwrite.bytebuffer.per.size",102400);
    blockSize = config.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
  }

  private void init() {
    FsDatasetSpi<?> fsDataset;
    boolean bIn = false;
    while (true) {
      fsDataset =  dataNode.getFSDataset();
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        bIn = true;
      }
      if (fsDataset != null) {
        break;
      }
    }

    volumes = fsDataset.getVolumes();
    //String localDirs = config.get("dfs.datanode.data.dir");
    BPOfferService[] bpos = dataNode.getAllBpOs();
    blockPoolID = bpos[0].getBlockPoolId();

    nDirs = volumes.size();
    finalizedDirs = new File[nDirs];
    baseDirs = new String[nDirs];
    blockTempDirs = new String[nDirs];
    storageIDs = new String[nDirs];

    try {
      for (int i = 0; i < volumes.size(); i++) {
        finalizedDirs[i] = volumes.get(i).getFinalizedDir(blockPoolID);
        baseDirs[i] = volumes.get(i).getBasePath();
        blockTempDirs[i] = baseDirs[i]; // + "/" + BLOCK_TMP_DIR;
        storageIDs[i] = volumes.get(i).getStorageID();
      }
    } catch (IOException e) {
      LOG.error("[SCW] Error in short circuit write internal initialization:" + e);
      shutdown = true;
    }
  }

  public void shutdownServer() {
        shutdown = true;
    }

  private void startServer(int port) {
    try {
      ServerSocketChannel ssc = ServerSocketChannel.open();
      Selector accSel = Selector.open();
      ServerSocket socket = ssc.socket();
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress(port));
      ssc.configureBlocking(false);
      ssc.register(accSel, SelectionKey.OP_ACCEPT);

      while (ssc.isOpen() && !shutdown) {
        if (accSel.select(1000) > 0) {
          Iterator<SelectionKey> it = accSel.selectedKeys().iterator();
          while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();
            if (key.isAcceptable()) {
                handleAccept(key);
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.error("[SCW] Failed in SCW startServer on Port " + port + " :", e);
    }
  }

  private void handleAccept(SelectionKey key) {
    try {
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel client = server.accept();
      if (client != null) {
        cachedThreadPool.execute(new WriteHandler(client, blockIndex++));
      }
    } catch (IOException e) {
      LOG.error("[SCW] Failed in SCW handleAccept:", e);
    }
  }
  private static boolean used = false;

  @Override
  public void run() {
    init();
    startServer(8899);
  }

  class WriteHandler implements Runnable {
    public final int BUFFER_SIZE = per_buffer_size;

    private SocketChannel sc;
    private ByteBuffer bb;

    private File blockTempFile;
    private File blockMetaTempFile;

    private ExtendedBlock block;

    private long dataLen = 0;

    private int currBlockIndex;
    private int volIndex;
    private long blockID;
    private long blockGS;



    WriteHandler(SocketChannel sc, int index) {
      this.sc = sc;
      bb = ByteBuffer.allocate(BUFFER_SIZE);
      currBlockIndex = index;
      volIndex = currBlockIndex % nDirs;
    }

    @Override
    public void run() {
      try {
        addBlock();
      } catch (IOException e) {
        LOG.error("[SCW] Error in write replica: " + e);
      }
    }

    private void addBlock() throws IOException {
      long start = Time.monotonicNow();

      writeFile();    // write block data file
//      long begin = Time.monotonicNow();
      writeMetaFile();

      if (dataLen == 0) {
        dataLen = blockTempFile.length();
      }

      ReplicaBeingWritten rbwReplica = new ReplicaBeingWritten(blockID, blockGS, volumes.get(volIndex), new File(blockTempDirs[volIndex]), 0);
      rbwReplica.setNumBytes(dataLen);

      FinalizedReplica finalizedReplica = ((FsDatasetImpl) (dataNode.data)).finalizeScwBlock(blockPoolID, rbwReplica, blockTempFile);

      //update metrics
      dataNode.metrics.addWriteBlockOp(Time.monotonicNow() - start);
      dataNode.metrics.incrWritesFromClient(true, dataLen);

      block.setNumBytes(finalizedReplica.getBlockFile().length());  // update block length
      dataNode.closeBlock(block, "", storageIDs[volIndex]);

//      LOG.info("total time write file used:"+(Time.monotonicNow()-begin));
//      LOG.info("[SCW] Write block successfully: " + block);
    }

    private void writeMetaFile() throws IOException {
      blockMetaTempFile = new File(DatanodeUtil.getMetaName(blockTempFile.getAbsolutePath(), blockGS));
      try {
        FileOutputStream osMeta = new FileOutputStream(blockMetaTempFile, false);
        osMeta.write(META_DATA);
        osMeta.close();
      } catch (FileNotFoundException ne) {
        throw new IOException(ne);
      }
    }

    private void writeFile() throws IOException {
      int readed = 0;
      RandomAccessFile fos = null;
      FileChannel fc = null;
      if (sc.isConnected()) {
        try {
          InputStream is = sc.socket().getInputStream();
          DataInputStream dis = new DataInputStream(is);
          long tempBlockID = dis.readLong();
          blockID = tempBlockID > 0 ? tempBlockID : -tempBlockID;
          blockGS = dis.readLong();

          block = new ExtendedBlock(blockPoolID, blockID, blockSize, blockGS);

          blockTempFile = new File(blockTempDirs[volIndex], "blk_" + blockID);

          if (tempBlockID < 0) {
            byte[] fn = blockTempFile.getAbsolutePath().getBytes("UTF-8");
            ByteBuffer len = ByteBuffer.allocate(4).putInt(fn.length);
            len.flip();
            while(len.hasRemaining()){
              sc.write(len);
            }
            ByteBuffer pathBuffer = ByteBuffer.wrap(fn);
            while(pathBuffer.hasRemaining()){
              sc.write(pathBuffer);
            }

            dataNode.notifyNamenodeReceivingBlock(block, storageIDs[volIndex]);

            len.flip();
            sc.read(len);
            sc.close();
            return;
          }

          dataNode.notifyNamenodeReceivingBlock(block, storageIDs[volIndex]);

          File file = blockTempFile;
          fos = new RandomAccessFile(file, "rw");
          fc = fos.getChannel();
          //fc = FileChannel.open(filePath, EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
          //LOG.debug("[SCW] Writing file " + file + " ...");

//          long read_time = 0L;
//          long write_time = 0L;
          while (true) {
            readed = 0;
//            long begin = Time.monotonicNow();
            while (bb.hasRemaining() && readed >= 0) {
              readed = sc.read(bb);
            }
//            read_time += Time.monotonicNow() - begin;

            bb.flip();

            //dataLen += bb.remaining();

//            begin = Time.monotonicNow();
            while (bb.hasRemaining()) {
              dataLen += fc.write(bb);
            }
//            write_time+=Time.monotonicNow()- begin;

            if (readed < 0) {
              break;
            }
            bb.flip();
          }

          fos.close();
          sc.close();
//          LOG.info("File read time is:"+read_time);
//          LOG.info("File write time is:"+write_time);
//          LOG.info("[SCW] Write file " + file + " finished with " + dataLen + " bytes!");
        } catch (IOException e) {
          LOG.error("[SCW] Write file " + blockID + " " + blockGS + " " + dataLen, e);
          //e.printStackTrace();
          throw e;
        }
      }
    }
  }
}
