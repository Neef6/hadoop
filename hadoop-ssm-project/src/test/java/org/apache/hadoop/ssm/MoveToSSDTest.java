package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by cc on 17-1-12.
 */
public class MoveToSSDTest {
//  private static final Log LOG = LogFactory.getLog(TestDFSAdmin.class);
//  private Configuration conf = null;
//  private MiniDFSCluster cluster;
//  private DFSAdmin admin;
//  private DataNode datanode;
//  private NameNode namenode;
//  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
//  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
//  private static final PrintStream OLD_OUT = System.out;
//  private static final PrintStream OLD_ERR = System.err;


//  private void redirectStream() {
//    System.setOut(new PrintStream(out));
//    System.setErr(new PrintStream(err));
//  }
//
//  @Before
//  public void setUp() throws Exception {
//    conf = new Configuration();
//    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 3);
//    restartCluster();
//
//    admin = new DFSAdmin();
//  }
//
//  private void restartCluster() throws IOException {
//    if (cluster != null) {
//      cluster.shutdown();
//    }
//    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
//    cluster.waitActive();
//    datanode = cluster.getDataNodes().get(0);
//    namenode = cluster.getNameNode();
//  }
//
//
//  @After
//  public void tearDown() throws Exception {
//    try {
//      System.out.flush();
//      System.err.flush();
//    } finally {
//      System.setOut(OLD_OUT);
//      System.setErr(OLD_ERR);
//    }
//
//    if (cluster != null) {
//      cluster.shutdown();
//      cluster = null;
//    }
//
//    resetStream();
//  }
//
//  private void resetStream() {
//    out.reset();
//    err.reset();
//  }

  static void initConf(Configuration conf) {
    final int DEFAULT_BLOCK_SIZE = 100;
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
            1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  @Test
  public void test1() throws IOException {
//    redirectStream();
    /* init conf */
    final Configuration dfsConf = new HdfsConfiguration();
    final File baseDir = new File(
            PathUtils.getTestDir(getClass()).getAbsolutePath(),
            GenericTestUtils.getMethodName());
    //dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());//baseDir.toString());
    initConf(dfsConf);

    final int numDn = 3;
    /* init cluster */

    try(MiniDFSCluster miniCluster = new MiniDFSCluster.Builder(dfsConf).numDataNodes(numDn).build()) {
      miniCluster.waitActive();
      assertEquals(numDn, miniCluster.getDataNodes().size());

      /* local vars */
//    final DFSAdmin dfsAdmin = new DFSAdmin(dfsConf);
      final DFSClient client = miniCluster.getFileSystem().getClient();
      //create a file
      final short replFactor = 1;
      final long fileLength = 512L;
      final FileSystem fs = miniCluster.getFileSystem();
//      final Path file = new Path("/testfile");
//      DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);

      //move to ssd
      ActionType actionType = ActionType.getActionType("ssd");

      String[] str = {"testfile1"};

      MoveToSSD_copy.getInstance(client,dfsConf).initial(str);
      MoveToSSD_copy.getInstance(client,dfsConf).execute();
      byte by = client.getFileInfo("testfile").getStoragePolicy();
      byte allSSDbyte = 12;
      assertEquals(allSSDbyte, client.getFileInfo("testfile").getStoragePolicy());
    } catch (IOException ioe) {

//    } finally {
//      if (miniCluster != null) {
//        miniCluster.shutdown();
//      }
    }

  }
}
//
//    DFSTestUtil.waitReplication(fs, file, replFactor);
//
//    final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file);
//    final int blockFilesCorrupted = miniCluster
//            .corruptBlockOnDataNodes(block);
//    assertEquals("Fail to corrupt all replicas for block " + block,
//            replFactor, blockFilesCorrupted);
//
//    try {
//      IOUtils.copyBytes(fs.open(file), new IOUtils.NullOutputStream(),
//              conf, true);
//      fail("Should have failed to read the file with corrupted blocks.");
//    } catch (ChecksumException ignored) {
//      // expected exception reading corrupt blocks
//    }
//
//      /*
//       * Increase replication factor, this should invoke transfer request.
//       * Receiving datanode fails on checksum and reports it to namenode
//       */
//    fs.setReplication(file, (short) (replFactor + 1));
//
//      /* get block details and check if the block is corrupt */
//    GenericTestUtils.waitFor(new Supplier<Boolean>() {
//      @Override
//      public Boolean get() {
//        LocatedBlocks blocks = null;
//        try {
//          miniCluster.triggerBlockReports();
//          blocks = client.getNamenode().getBlockLocations(file.toString(), 0,
//                  Long.MAX_VALUE);
//        } catch (IOException e) {
//          return false;
//        }
//        return blocks != null && blocks.get(0).isCorrupt();
//      }
//    }, 1000, 60000);
//
//    BlockManagerTestUtil.updateState(
//            miniCluster.getNameNode().getNamesystem().getBlockManager());
//
//      /* run and verify report command */
//    resetStream();
//    assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
//    verifyNodesAndCorruptBlocks(numDn, numDn - 1, 1, client);
//  }
//
//  public MoveToSSDTest() throws IOException {
//  }
//
//}
//
//  private void verifyNodesAndCorruptBlocks(
//          final int numDn,
//          final int numLiveDn,
//          final int numCorruptBlocks,
//          final DFSClient client) throws IOException {
//
//    /* init vars */
//    final String outStr = scanIntoString(out);
//    final String expectedLiveNodesStr = String.format(
//            "Live datanodes (%d)",
//            numLiveDn);
//    final String expectedCorruptedBlocksStr = String.format(
//            "Blocks with corrupt replicas: %d",
//            numCorruptBlocks);
//
//    /* verify nodes and corrupt blocks */
//    assertThat(outStr, is(allOf(
//            containsString(expectedLiveNodesStr),
//            containsString(expectedCorruptedBlocksStr))));
//
//    assertEquals(
//            numDn,
//            client.getDatanodeStorageReport(HdfsConstants.DatanodeReportType.ALL).length);
//    assertEquals(
//            numLiveDn,
//            client.getDatanodeStorageReport(HdfsConstants.DatanodeReportType.LIVE).length);
//    assertEquals(
//            numDn - numLiveDn,
//            client.getDatanodeStorageReport(HdfsConstants.DatanodeReportType.DEAD).length);
//    assertEquals(numCorruptBlocks, client.getCorruptBlocksCount());
//  }
//  private void redirectStream() {
//    System.setOut(new PrintStream(out));
//    System.setErr(new PrintStream(err));
//  }



//  @Test
//  public void testMkdir()throws IOException {
//    Configuration conf = new Configuration();
//    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
//    final DFSClient client = cluster.getFileSystem().getClient();
//
//    final Path baseDir = new Path(PathUtils.getTestDir(getClass()).getAbsolutePath(), GenericTestUtils.getMethodName());
//
//    File filename = new File("testFile");
//    String path = filename.getAbsolutePath();
//
//    DistributedFileSystem fs = cluster.getFileSystem();
////    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/testfile");
//    DFSTestUtil.createFile(fs, path, 1024, (short) 3, 0);
////    DFSInputStream fin = fs.getClient().open("/testfile");
////
////    ActionType actionType = ActionType.getActionType("ssd");
//
//    String[] str = {"fileTestA"};
//    MoveToSSD moveToSSD=new MoveToSSD(client,conf,"testfile") ;
//
//    moveToSSD.initial(str);
//    moveToSSD.execute();
//
//    assertEquals(StorageType.SSD,client.getStoragePolicy("/testfile").getStorageTypes());
//  }
//  }
//
//    /* init conf */
//    final Configuration dfsConf = new HdfsConfiguration();
//    dfsConf.setInt(
//            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
//            500); // 0.5s
//    dfsConf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
//    final Path baseDir = new Path(
//            PathUtils.getTestDir(getClass()).getAbsolutePath(),
//            GenericTestUtils.getMethodName());
//    dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());
//
//    final int numDn = 3;
//
//    /* init cluster */
//    try(MiniDFSCluster miniCluster = new MiniDFSCluster
//            .Builder(dfsConf)
//            .numDataNodes(numDn).build()) {
//
//      miniCluster.waitActive();
//      assertEquals(numDn, miniCluster.getDataNodes().size());