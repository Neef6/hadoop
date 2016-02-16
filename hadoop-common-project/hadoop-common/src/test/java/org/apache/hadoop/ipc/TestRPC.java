/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ipc;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.test.MockitoUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import javax.net.SocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertCounterGt;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/** Unit tests for RPC. */
@SuppressWarnings("deprecation")
public class TestRPC  extends TestRpcBase {

  public static final Log LOG = LogFactory.getLog(TestRPC.class);

  @Before
  public void setup() {
    setupConf();

    RPC.setProtocolEngine(conf,
        StoppedProtocol.class, StoppedRpcEngine.class);
  }

  int datasize = 1024*100;
  int numThreads = 50;

  public interface TestProtocol extends VersionedProtocol {
    long versionID = 1L;

    void ping() throws IOException;
    void sleep(long delay) throws IOException, InterruptedException;
    String echo(String value) throws IOException;
    String[] echo(String[] value) throws IOException;
    Writable echo(Writable value) throws IOException;
    int add(int v1, int v2) throws IOException;
    int add(int[] values) throws IOException;
    int error() throws IOException;
    void testServerGet() throws IOException;
    int[] exchange(int[] values) throws IOException;

    DescriptorProtos.EnumDescriptorProto exchangeProto(
        DescriptorProtos.EnumDescriptorProto arg);
  }

  public static class TestImpl implements TestProtocol {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) {
      return TestProtocol.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
                                                  int hashcode) {
      return new ProtocolSignature(TestProtocol.versionID, null);
    }

    @Override
    public void ping() {}

    @Override
    public void sleep(long delay) throws InterruptedException {
      Thread.sleep(delay);
    }

    @Override
    public String echo(String value) throws IOException { return value; }

    @Override
    public String[] echo(String[] values) throws IOException { return values; }

    @Override
    public Writable echo(Writable writable) {
      return writable;
    }
    @Override
    public int add(int v1, int v2) {
      return v1 + v2;
    }

    @Override
    public int add(int[] values) {
      int sum = 0;
      for (int i = 0; i < values.length; i++) {
        sum += values[i];
      }
      return sum;
    }

    @Override
    public int error() throws IOException {
      throw new IOException("bobo");
    }

    @Override
    public void testServerGet() throws IOException {
      if (!(Server.get() instanceof RPC.Server)) {
        throw new IOException("Server.get() failed");
      }
    }

    @Override
    public int[] exchange(int[] values) {
      for (int i = 0; i < values.length; i++) {
        values[i] = i;
      }
      return values;
    }

    @Override
    public EnumDescriptorProto exchangeProto(EnumDescriptorProto arg) {
      return arg;
    }
  }

  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    int datasize;
    TestRpcService proxy;

    Transactions(TestRpcService proxy, int datasize) {
      this.proxy = proxy;
      this.datasize = datasize;
    }

    // do two RPC that transfers data.
    @Override
    public void run() {
      Integer[] indata = new Integer[datasize];
      Arrays.fill(indata, 123);
      TestProtos.ExchangeRequestProto exchangeRequest =
          TestProtos.ExchangeRequestProto.newBuilder().addAllValues(
              Arrays.asList(indata)).build();
      Integer[] outdata = null;
      TestProtos.ExchangeResponseProto exchangeResponse;

      TestProtos.AddRequestProto addRequest =
          TestProtos.AddRequestProto.newBuilder().setParam1(1)
              .setParam2(2).build();
      TestProtos.AddResponseProto addResponse;

      int val = 0;
      try {
        exchangeResponse = proxy.exchange(null, exchangeRequest);
        outdata = new Integer[exchangeResponse.getValuesCount()];
        outdata = exchangeResponse.getValuesList().toArray(outdata);
        addResponse = proxy.add(null, addRequest);
        val = addResponse.getResult();
      } catch (ServiceException e) {
        assertTrue("Exception from RPC exchange() "  + e, false);
      }
      assertEquals(indata.length, outdata.length);
      assertEquals(3, val);
      for (int i = 0; i < outdata.length; i++) {
        assertEquals(outdata[i].intValue(), i);
      }
    }
  }

  //
  // A class that does an RPC but does not read its response.
  //
  static class SlowRPC implements Runnable {
    private TestRpcService proxy;
    private volatile boolean done;

    SlowRPC(TestRpcService proxy) {
      this.proxy = proxy;
      done = false;
    }

    boolean isDone() {
      return done;
    }

    @Override
    public void run() {
      try {
        // this would hang until two fast pings happened
        ping(true);
        done = true;
      } catch (ServiceException e) {
        assertTrue("SlowRPC ping exception " + e, false);
      }
    }

    void ping(boolean shouldSlow) throws ServiceException {
      TestProtos.SlowPingRequestProto slowPingRequest =
          TestProtos.SlowPingRequestProto.newBuilder().
              setShouldSlow(shouldSlow).build();
      // this would hang until two fast pings happened
      proxy.slowPing(null, slowPingRequest);
    }
  }

  /**
   * A basic interface for testing client-side RPC resource cleanup.
   */
  private interface StoppedProtocol {
    long versionID = 0;

    void stop();
  }

  /**
   * A class used for testing cleanup of client side RPC resources.
   */
  private static class StoppedRpcEngine implements RpcEngine {

    @Override
    public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
                                         InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
                                         SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy
    ) throws IOException {
      return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
          rpcTimeout, connectionRetryPolicy, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
                                         InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
                                         SocketFactory factory, int rpcTimeout,
                                         RetryPolicy connectionRetryPolicy, AtomicBoolean fallbackToSimpleAuth
    ) throws IOException {
      T proxy = (T) Proxy.newProxyInstance(protocol.getClassLoader(),
          new Class[] { protocol }, new StoppedInvocationHandler());
      return new ProtocolProxy<T>(protocol, proxy, false);
    }

    @Override
    public org.apache.hadoop.ipc.RPC.Server getServer(Class<?> protocol,
                                                      Object instance, String bindAddress, int port, int numHandlers,
                                                      int numReaders, int queueSizePerHandler, boolean verbose, Configuration conf,
                                                      SecretManager<? extends TokenIdentifier> secretManager,
                                                      String portRangeConfig) throws IOException {
      return null;
    }

    @Override
    public ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
        ConnectionId connId, Configuration conf, SocketFactory factory)
        throws IOException {
      throw new UnsupportedOperationException("This proxy is not supported");
    }
  }

  /**
   * An invocation handler which does nothing when invoking methods, and just
   * counts the number of times close() is called.
   */
  private static class StoppedInvocationHandler
      implements InvocationHandler, Closeable {

    private int closeCalled = 0;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      return null;
    }

    @Override
    public void close() throws IOException {
      closeCalled++;
    }

    public int getCloseCalled() {
      return closeCalled;
    }

  }

  @Test
  public void testConfRpc() throws IOException {
    Server server = setupTestServer(conf, 1);
    // Just one handler
    int confQ = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
    assertEquals(confQ, server.getMaxQueueSize());

    int confReaders = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
    assertEquals(confReaders, server.getNumReaders());
    server.stop();

    server = newServerBuilder(conf)
        .setNumHandlers(1).setnumReaders(3).setQueueSizePerHandler(200)
        .setVerbose(false).build();

    assertEquals(3, server.getNumReaders());
    assertEquals(200, server.getMaxQueueSize());
    server.stop();
  }

  @Test
  public void testProxyAddress() throws IOException {
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0).build();
    TestProtocol proxy = null;

    try {
      server.start();
      InetSocketAddress addr = NetUtils.getConnectAddress(server);

      // create a client
      proxy = RPC.getProxy(TestProtocol.class, TestProtocol.versionID, addr, conf);

      assertEquals(addr, RPC.getServerAddress(proxy));
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }

  @Test
  public void testSlowRpc() throws IOException, ServiceException {
    System.out.println("Testing Slow RPC");
    // create a server with two handlers
    Server server = setupTestServer(conf, 2);

    TestRpcService proxy = null;

    try {
      server.start();

      // create a client
      proxy = getClient();

      SlowRPC slowrpc = new SlowRPC(proxy);
      Thread thread = new Thread(slowrpc, "SlowRPC");
      thread.start(); // send a slow RPC, which won't return until two fast pings
      assertTrue("Slow RPC should not have finished1.", !slowrpc.isDone());

      slowrpc.ping(false); // first fast ping

      // verify that the first RPC is still stuck
      assertTrue("Slow RPC should not have finished2.", !slowrpc.isDone());

      slowrpc.ping(false); // second fast ping

      // Now the slow ping should be able to be executed
      while (!slowrpc.isDone()) {
        System.out.println("Waiting for slow RPC to get done.");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {}
      }
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      System.out.println("Down slow rpc testing");
    }
  }

  @Test
  public void testCalls() throws Exception {
    testCallsInternal(conf);
  }

  private void testCallsInternal(Configuration conf) throws Exception {
    Server server = setupTestServer(conf, -1);
    TestRpcService proxy = null;
    try {
      server.start();

      proxy = getClient();

      TestProtos.EmptyRequestProto pingRequest =
          TestProtos.EmptyRequestProto.newBuilder().build();
      proxy.ping(null, pingRequest);

      TestProtos.EchoRequestProto echoRequest =
          TestProtos.EchoRequestProto.newBuilder()
              .setMessage("foo").build();
      TestProtos.EchoResponseProto echoResp = proxy.echo(null, echoRequest);
      assertEquals(echoResp.getMessage(), "foo");

      echoRequest = TestProtos.EchoRequestProto.newBuilder()
          .setMessage("").build();
      echoResp = proxy.echo(null, echoRequest);
      assertEquals(echoResp.getMessage(), "");

      // Check rpcMetrics
      MetricsRecordBuilder rb = getMetrics(server.rpcMetrics.name());
      assertCounter("RpcProcessingTimeNumOps", 3L, rb);
      assertCounterGt("SentBytes", 0L, rb);
      assertCounterGt("ReceivedBytes", 0L, rb);

      // Number of calls to echo method should be 2
      rb = getMetrics(server.rpcDetailedMetrics.name());
      assertCounter("EchoNumOps", 2L, rb);

      // Number of calls to ping method should be 1
      assertCounter("PingNumOps", 1L, rb);

      String[] strings = new String[] {"foo","bar"};
      TestProtos.EchoRequestProto2 echoRequest2 =
          TestProtos.EchoRequestProto2.newBuilder().addAllMessage(
              Arrays.asList(strings)).build();
      TestProtos.EchoResponseProto2 echoResponse2 =
          proxy.echo2(null, echoRequest2);
      assertTrue(Arrays.equals(echoResponse2.getMessageList().toArray(),
          strings));

      echoRequest2 = TestProtos.EchoRequestProto2.newBuilder()
          .addAllMessage(Collections.<String>emptyList()).build();
      echoResponse2 = proxy.echo2(null, echoRequest2);
      assertTrue(Arrays.equals(echoResponse2.getMessageList().toArray(),
          new String[]{}));

      TestProtos.AddRequestProto addRequest =
          TestProtos.AddRequestProto.newBuilder().setParam1(1)
          .setParam2(2).build();
      TestProtos.AddResponseProto addResponse =
          proxy.add(null, addRequest);
      assertEquals(addResponse.getResult(), 3);

      Integer[] integers = new Integer[] {1, 2};
      TestProtos.AddRequestProto2 addRequest2 =
          TestProtos.AddRequestProto2.newBuilder().addAllParams(
              Arrays.asList(integers)).build();
      addResponse = proxy.add2(null, addRequest2);
      assertEquals(addResponse.getResult(), 3);

      boolean caught = false;
      TestProtos.EmptyRequestProto emptyRequest =
          TestProtos.EmptyRequestProto.newBuilder().build();
      try {
        proxy.error(null, emptyRequest);
      } catch (ServiceException e) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Caught " + e);
        }
        caught = true;
      }
      assertTrue(caught);
      rb = getMetrics(server.rpcDetailedMetrics.name());
      assertCounter("RpcServerExceptionNumOps", 1L, rb);

      //proxy.testServerGet();

      // create multiple threads and make them do large data transfers
      System.out.println("Starting multi-threaded RPC test...");
      server.setSocketSendBufSize(1024);
      Thread threadId[] = new Thread[numThreads];
      for (int i = 0; i < numThreads; i++) {
        Transactions trans = new Transactions(proxy, datasize);
        threadId[i] = new Thread(trans, "TransactionThread-" + i);
        threadId[i].start();
      }

      // wait for all transactions to get over
      System.out.println("Waiting for all threads to finish RPCs...");
      for (int i = 0; i < numThreads; i++) {
        try {
          threadId[i].join();
        } catch (InterruptedException e) {
          i--;      // retry
        }
      }

    } finally {
      server.stop();
      if(proxy!=null) RPC.stopProxy(proxy);
    }
  }

  @Test
  public void testClientWithoutServer() throws Exception {
    short invalidPort = 20;
    InetSocketAddress invalidAddress = new InetSocketAddress(ADDRESS,
        invalidPort);
    long invalidClientVersion = 1L;
    try {
      TestRpcService proxy = RPC.getProxy(TestRpcService.class,
          invalidClientVersion, invalidAddress, conf);
      // Test echo method
      TestProtos.EchoRequestProto echoRequest =
          TestProtos.EchoRequestProto.newBuilder().setMessage("hello").build();
      proxy.echo(null, echoRequest);
      fail("We should not have reached here");
    } catch (ServiceException ioe) {
      //this is what we expected
      if (!(ioe.getCause() instanceof ConnectException)) {
        fail("We should not have reached here");
      }
    }
  }

  private static final String ACL_CONFIG = "test.protocol.acl";

  private static class TestPolicyProvider extends PolicyProvider {

    @Override
    public Service[] getServices() {
      return new Service[] { new Service(ACL_CONFIG, TestRpcService.class) };
    }

  }

  private void doRPCs(Configuration conf, boolean expectFailure) throws Exception {
    Server server = setupTestServer(conf, 5);

    server.refreshServiceAcl(conf, new TestPolicyProvider());

    server.start();

    TestRpcService proxy = getClient();
    TestProtos.EmptyRequestProto emptyRequestProto =
        TestProtos.EmptyRequestProto.newBuilder().build();

    try {
      proxy.ping(null, emptyRequestProto);
      if (expectFailure) {
        fail("Expect RPC.getProxy to fail with AuthorizationException!");
      }
    } catch (ServiceException e) {
      if (expectFailure) {
        RemoteException re = (RemoteException) e.getCause();
        assertTrue(AuthorizationException.class.getName().equals(
            re.getClassName()));
        assertEquals("RPC error code should be UNAUTHORIZED",
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, re.getErrorCode());
      } else {
        throw e;
      }
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      MetricsRecordBuilder rb = getMetrics(server.rpcMetrics.name());
      if (expectFailure) {
        assertCounter("RpcAuthorizationFailures", 1L, rb);
      } else {
        assertCounter("RpcAuthorizationSuccesses", 1L, rb);
      }
      //since we don't have authentication turned ON, we should see 
      // 0 for the authentication successes and 0 for failure
      assertCounter("RpcAuthenticationFailures", 0L, rb);
      assertCounter("RpcAuthenticationSuccesses", 0L, rb);
    }
  }

  @Test
  public void testServerAddress() throws IOException {
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true).build();
    InetSocketAddress bindAddr = null;
    try {
      bindAddr = NetUtils.getConnectAddress(server);
    } finally {
      server.stop();
    }
    assertEquals(InetAddress.getLocalHost(), bindAddr.getAddress());
  }

  @Test
  public void testAuthorization() throws Exception {
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);

    // Expect to succeed
    conf.set(ACL_CONFIG, "*");
    doRPCs(conf, false);

    // Reset authorization to expect failure
    conf.set(ACL_CONFIG, "invalid invalid");
    doRPCs(conf, true);

    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);
    // Expect to succeed
    conf.set(ACL_CONFIG, "*");
    doRPCs(conf, false);

    // Reset authorization to expect failure
    conf.set(ACL_CONFIG, "invalid invalid");
    doRPCs(conf, true);
  }

  /**
   * Switch off setting socketTimeout values on RPC sockets.
   * Verify that RPC calls still work ok.
   */
  public void testNoPings() throws Exception {
    Configuration conf = new Configuration();

    conf.setBoolean("ipc.client.ping", false);
    new TestRPC().testCallsInternal(conf);

    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);
    new TestRPC().testCallsInternal(conf);
  }

  /**
   * Test stopping a non-registered proxy
   * @throws IOException
   */
  @Test(expected=HadoopIllegalArgumentException.class)
  public void testStopNonRegisteredProxy() throws IOException {
    RPC.stopProxy(null);
  }

  /**
   * Test that the mockProtocol helper returns mock proxies that can
   * be stopped without error.
   */
  @Test
  public void testStopMockObject() throws IOException {
    RPC.stopProxy(MockitoUtil.mockProtocol(TestProtocol.class));
  }

  @Test
  public void testStopProxy() throws IOException {
    StoppedProtocol proxy = RPC.getProxy(StoppedProtocol.class,
        StoppedProtocol.versionID, null, conf);
    StoppedInvocationHandler invocationHandler = (StoppedInvocationHandler)
        Proxy.getInvocationHandler(proxy);
    assertEquals(0, invocationHandler.getCloseCalled());
    RPC.stopProxy(proxy);
    assertEquals(1, invocationHandler.getCloseCalled());
  }

  @Test
  public void testWrappedStopProxy() throws IOException {
    StoppedProtocol wrappedProxy = RPC.getProxy(StoppedProtocol.class,
        StoppedProtocol.versionID, null, conf);
    StoppedInvocationHandler invocationHandler = (StoppedInvocationHandler)
        Proxy.getInvocationHandler(wrappedProxy);

    StoppedProtocol proxy = (StoppedProtocol) RetryProxy.create(StoppedProtocol.class,
        wrappedProxy, RetryPolicies.RETRY_FOREVER);

    assertEquals(0, invocationHandler.getCloseCalled());
    RPC.stopProxy(proxy);
    assertEquals(1, invocationHandler.getCloseCalled());
  }

  @Test
  public void testErrorMsgForInsecureClient() throws IOException {
    Configuration serverConf = new Configuration(conf);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS,
        serverConf);
    UserGroupInformation.setConfiguration(serverConf);

    final Server server = new RPC.Builder(serverConf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true).build();
    server.start();

    UserGroupInformation.setConfiguration(conf);
    boolean succeeded = false;
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    TestProtocol proxy = null;
    try {
      proxy = RPC.getProxy(TestProtocol.class, TestProtocol.versionID, addr, conf);
      proxy.echo("");
    } catch (RemoteException e) {
      LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
      assertEquals("RPC error code should be UNAUTHORIZED", RpcErrorCodeProto.FATAL_UNAUTHORIZED, e.getErrorCode());
      assertTrue(e.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
    assertTrue(succeeded);

    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);

    UserGroupInformation.setConfiguration(serverConf);
    final Server multiServer = new RPC.Builder(serverConf)
        .setProtocol(TestProtocol.class).setInstance(new TestImpl())
        .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5).setVerbose(true)
        .build();
    multiServer.start();
    succeeded = false;
    final InetSocketAddress mulitServerAddr =
        NetUtils.getConnectAddress(multiServer);
    proxy = null;
    try {
      UserGroupInformation.setConfiguration(conf);
      proxy = RPC.getProxy(TestProtocol.class,
          TestProtocol.versionID, mulitServerAddr, conf);
      proxy.echo("");
    } catch (RemoteException e) {
      LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
      assertEquals("RPC error code should be UNAUTHORIZED", RpcErrorCodeProto.FATAL_UNAUTHORIZED, e.getErrorCode());
      assertTrue(e.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {
      multiServer.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
    assertTrue(succeeded);
  }

  /**
   * Count the number of threads that have a stack frame containing
   * the given string
   */
  private static int countThreads(String search) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    int count = 0;
    ThreadInfo[] infos = threadBean.getThreadInfo(threadBean.getAllThreadIds(), 20);
    for (ThreadInfo info : infos) {
      if (info == null) continue;
      for (StackTraceElement elem : info.getStackTrace()) {
        if (elem.getClassName().contains(search)) {
          count++;
          break;
        }
      }
    }
    return count;
  }

  /**
   * Test that server.stop() properly stops all threads
   */
  @Test
  public void testStopsAllThreads() throws IOException, InterruptedException {
    int threadsBefore = countThreads("Server$Listener$Reader");
    assertEquals("Expect no Reader threads running before test",
        0, threadsBefore);

    final Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true).build();
    server.start();
    try {
      // Wait for at least one reader thread to start
      int threadsRunning = 0;
      long totalSleepTime = 0;
      do {
        totalSleepTime += 10;
        Thread.sleep(10);
        threadsRunning = countThreads("Server$Listener$Reader");
      } while (threadsRunning == 0 && totalSleepTime < 5000);

      // Validate that at least one thread started (we didn't timeout)
      threadsRunning = countThreads("Server$Listener$Reader");
      assertTrue(threadsRunning > 0);
    } finally {
      server.stop();
    }
    int threadsAfter = countThreads("Server$Listener$Reader");
    assertEquals("Expect no Reader threads left running after test",
        0, threadsAfter);
  }

  @Test
  public void testRPCBuilder() throws IOException {
    // Test mandatory field conf
    try {
      new RPC.Builder(null).setProtocol(TestProtocol.class)
          .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
          .setNumHandlers(5).setVerbose(true).build();
      fail("Didn't throw HadoopIllegalArgumentException");
    } catch (Exception e) {
      if (!(e instanceof HadoopIllegalArgumentException)) {
        fail("Expecting HadoopIllegalArgumentException but caught " + e);
      }
    }
    // Test mandatory field protocol
    try {
      new RPC.Builder(conf).setInstance(new TestImpl()).setBindAddress(ADDRESS)
          .setPort(0).setNumHandlers(5).setVerbose(true).build();
      fail("Didn't throw HadoopIllegalArgumentException");
    } catch (Exception e) {
      if (!(e instanceof HadoopIllegalArgumentException)) {
        fail("Expecting HadoopIllegalArgumentException but caught " + e);
      }
    }
    // Test mandatory field instance
    try {
      new RPC.Builder(conf).setProtocol(TestProtocol.class)
          .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5)
          .setVerbose(true).build();
      fail("Didn't throw HadoopIllegalArgumentException");
    } catch (Exception e) {
      if (!(e instanceof HadoopIllegalArgumentException)) {
        fail("Expecting HadoopIllegalArgumentException but caught " + e);
      }
    }
  }

  @Test(timeout=90000)
  public void testRPCInterruptedSimple() throws IOException {
    final Configuration conf = new Configuration();
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS)
        .setPort(0).setNumHandlers(5).setVerbose(true)
        .setSecretManager(null).build();

    server.start();
    try {
      InetSocketAddress addr = NetUtils.getConnectAddress(server);

      final TestProtocol proxy = RPC.getProxy(
          TestProtocol.class, TestProtocol.versionID, addr, conf);
      // Connect to the server
      proxy.ping();
      // Interrupt self, try another call
      Thread.currentThread().interrupt();
      try {
        proxy.ping();
        fail("Interruption did not cause IPC to fail");
      } catch (IOException ioe) {
        if (ioe.toString().contains("InterruptedException") ||
            ioe instanceof InterruptedIOException) {
          // clear interrupt status for future tests
          Thread.interrupted();
          return;
        }
        throw ioe;
      }
    } finally {
      server.stop();
    }
  }

  @Test(timeout=30000)
  public void testRPCInterrupted() throws IOException, InterruptedException {
    Server server = setupTestServer(conf, 5);

    final TestProtos.SlowPingRequestProto slowPingRequest =
        TestProtos.SlowPingRequestProto.newBuilder().
            setShouldSlow(false).build();

    server.start();

    try {
      int numConcurrentRPC = 200;
      final CyclicBarrier barrier = new CyclicBarrier(numConcurrentRPC);
      final CountDownLatch latch = new CountDownLatch(numConcurrentRPC);
      final AtomicBoolean leaderRunning = new AtomicBoolean(true);
      final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
      Thread leaderThread = null;

      for (int i = 0; i < numConcurrentRPC; i++) {
        final int num = i;
        final TestRpcService proxy = getClient();
        Thread rpcThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              barrier.await();
              while (num == 0 || leaderRunning.get()) {
                proxy.slowPing(null, slowPingRequest);
              }

              proxy.slowPing(null, slowPingRequest);
            } catch (Exception e) {
              if (num == 0) {
                leaderRunning.set(false);
              } else {
                error.set(e);
              }

              LOG.error("thread " + num, e);
            } finally {
              latch.countDown();
            }
          }
        });
        rpcThread.start();

        if (leaderThread == null) {
          leaderThread = rpcThread;
        }
      }
      // let threads get past the barrier
      Thread.sleep(1000);
      // stop a single thread
      while (leaderRunning.get()) {
        leaderThread.interrupt();
      }

      latch.await();

      // should not cause any other thread to get an error
      assertTrue("rpc got exception " + error.get(), error.get() == null);
    } finally {
      server.stop();
    }
  }

  @Test
  public void testConnectionPing() throws Exception {
    int pingInterval = 50;
    conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
    RPC.Server server = setupTestServer(conf, 5);
    server.start();

    TestRpcService proxy = getClient();

    try {
      TestProtos.SleepRequestProto sleepRequestProto =
          TestProtos.SleepRequestProto.newBuilder()
              .setMilliSeconds(pingInterval * 4).build();
      proxy.sleep(null, sleepRequestProto);
    } finally {
      if (proxy != null) RPC.stopProxy(proxy);
      server.stop();
    }
  }

  @Test
  public void testRpcMetrics() throws Exception {
    Configuration configuration = new Configuration();
    final int interval = 1;
    configuration.setBoolean(CommonConfigurationKeys.
        RPC_METRICS_QUANTILE_ENABLE, true);
    configuration.set(CommonConfigurationKeys.
        RPC_METRICS_PERCENTILES_INTERVALS_KEY, "" + interval);
    final Server server = new RPC.Builder(configuration)
        .setProtocol(TestProtocol.class).setInstance(new TestImpl())
        .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5).setVerbose(true)
        .build();
    server.start();
    final TestProtocol proxy = RPC.getProxy(TestProtocol.class,
        TestProtocol.versionID, server.getListenerAddress(), configuration);
    try {
      for (int i=0; i<1000; i++) {
        proxy.ping();
        proxy.echo("" + i);
      }
      MetricsRecordBuilder rpcMetrics =
          getMetrics(server.getRpcMetrics().name());
      assertTrue("Expected non-zero rpc queue time",
          getLongCounter("RpcQueueTimeNumOps", rpcMetrics) > 0);
      assertTrue("Expected non-zero rpc processing time",
          getLongCounter("RpcProcessingTimeNumOps", rpcMetrics) > 0);
      MetricsAsserts.assertQuantileGauges("RpcQueueTime" + interval + "s",
          rpcMetrics);
      MetricsAsserts.assertQuantileGauges("RpcProcessingTime" + interval + "s",
          rpcMetrics);
    } finally {
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      server.stop();
    }
  }

  /**
   *  Verify the RPC server can shutdown properly when callQueue is full.
   */
  @Test (timeout=30000)
  public void testRPCServerShutdown() throws Exception {
    final int numClients = 3;
    final List<Future<Void>> res = new ArrayList<Future<Void>>();
    final ExecutorService executorService =
        Executors.newFixedThreadPool(numClients);
    final Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    final Server server = new RPC.Builder(conf)
        .setProtocol(TestProtocol.class).setInstance(new TestImpl())
        .setBindAddress(ADDRESS).setPort(0)
        .setQueueSizePerHandler(1).setNumHandlers(1).setVerbose(true)
        .build();
    server.start();

    final TestProtocol proxy =
        RPC.getProxy(TestProtocol.class, TestProtocol.versionID,
            NetUtils.getConnectAddress(server), conf);
    try {
      // start a sleep RPC call to consume the only handler thread.
      // Start another sleep RPC call to make callQueue full.
      // Start another sleep RPC call to make reader thread block on CallQueue.
      for (int i = 0; i < numClients; i++) {
        res.add(executorService.submit(
            new Callable<Void>() {
              @Override
              public Void call() throws IOException, InterruptedException {
                proxy.sleep(100000);
                return null;
              }
            }));
      }
      while (server.getCallQueueLen() != 1
          || countThreads(CallQueueManager.class.getName()) != 1
          || countThreads(TestImpl.class.getName()) != 1) {
        Thread.sleep(100);
      }
    } finally {
      try {
        server.stop();
        assertEquals("Not enough clients", numClients, res.size());
        for (Future<Void> f : res) {
          try {
            f.get();
            fail("Future get should not return");
          } catch (ExecutionException e) {
            assertTrue("Unexpected exception: " + e,
                e.getCause() instanceof IOException);
            LOG.info("Expected exception", e.getCause());
          }
        }
      } finally {
        RPC.stopProxy(proxy);
        executorService.shutdown();
      }
    }
  }

  /**
   *  Test RPC backoff.
   */
  @Test (timeout=30000)
  public void testClientBackOff() throws Exception {
    boolean succeeded = false;
    final int numClients = 2;
    final List<Future<Void>> res = new ArrayList<Future<Void>>();
    final ExecutorService executorService =
        Executors.newFixedThreadPool(numClients);
    final Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    conf.setBoolean(CommonConfigurationKeys.IPC_CALLQUEUE_NAMESPACE +
        ".0." + CommonConfigurationKeys.IPC_BACKOFF_ENABLE, true);
    final Server server = new RPC.Builder(conf)
        .setProtocol(TestProtocol.class).setInstance(new TestImpl())
        .setBindAddress(ADDRESS).setPort(0)
        .setQueueSizePerHandler(1).setNumHandlers(1).setVerbose(true)
        .build();
    @SuppressWarnings("unchecked")
    CallQueueManager<Call> spy = spy((CallQueueManager<Call>) Whitebox
        .getInternalState(server, "callQueue"));
    Whitebox.setInternalState(server, "callQueue", spy);
    server.start();

    Exception lastException = null;
    final TestProtocol proxy =
        RPC.getProxy(TestProtocol.class, TestProtocol.versionID,
            NetUtils.getConnectAddress(server), conf);
    try {
      // start a sleep RPC call to consume the only handler thread.
      // Start another sleep RPC call to make callQueue full.
      // Start another sleep RPC call to make reader thread block on CallQueue.
      for (int i = 0; i < numClients; i++) {
        res.add(executorService.submit(
            new Callable<Void>() {
              @Override
              public Void call() throws IOException, InterruptedException {
                proxy.sleep(100000);
                return null;
              }
            }));
        verify(spy, timeout(500).times(i + 1)).offer(Mockito.<Call>anyObject());
      }
      try {
        proxy.sleep(100);
      } catch (RemoteException e) {
        IOException unwrapExeption = e.unwrapRemoteException();
        if (unwrapExeption instanceof RetriableException) {
          succeeded = true;
        } else {
          lastException = unwrapExeption;
        }
      }
    } finally {
      server.stop();
      RPC.stopProxy(proxy);
      executorService.shutdown();
    }
    if (lastException != null) {
      LOG.error("Last received non-RetriableException:", lastException);
    }
    assertTrue("RetriableException not received", succeeded);
  }

  /**
   *  Test RPC timeout.
   */
  @Test(timeout=30000)
  public void testClientRpcTimeout() throws Exception {
    RPC.Builder builder = newServerBuilder(conf)
        .setQueueSizePerHandler(1).setNumHandlers(1).setVerbose(true);
    RPC.Server server = builder.build();
    server.start();
    addr = NetUtils.getConnectAddress(server);

    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 1000);
    TestRpcService proxy = getClient();

    try {
      TestProtos.SleepRequestProto sleepRequestProto =
          TestProtos.SleepRequestProto.newBuilder()
              .setMilliSeconds(3000).build();
      proxy.sleep(null, sleepRequestProto);
      fail("RPC should time out.");
    } catch (ServiceException e) {
      assertTrue(e.getCause() instanceof SocketTimeoutException);
      LOG.info("got expected timeout.", e);
    } finally {
      server.stop();
      RPC.stopProxy(proxy);
    }
  }

  public static void main(String[] args) throws Exception {
    new TestRPC().testCallsInternal(conf);
  }
}
