package com.latticeengines.camille;


public class CamilleEnvironmentUnitTestNG {

    // @SuppressWarnings("unused")
    // private static final Logger log = LoggerFactory.getLogger(new Object() {
    // }.getClass().getEnclosingClass());
    //
    // // @BeforeMethod(groups = "unit")
    // // public void setUp() throws Exception {
    // // CamilleTestEnvironment.start();
    // // }
    // //
    // // @AfterMethod(groups = "unit")
    // // public void tearDown() throws Exception {
    // // CamilleTestEnvironment.stop();
    // // }
    //
    // @Test(groups = "unit")
    // public synchronized void testBootstrapWithMultiplePods() throws Exception
    // {
    // try (TestingServer server = new TestingServer()) {
    //
    // CuratorFramework client =
    // CuratorFrameworkFactory.newClient(server.getConnectString(),
    // new ExponentialBackoffRetry(1000, 3));
    // client.start();
    // try {
    // client.blockUntilConnected();
    // } catch (InterruptedException ie) {
    // Assert.fail("There was a problem with the testing server.");
    // }
    //
    // CamilleEnvironment.stop();
    //
    // // create Pods path
    // Path podsPath = PathBuilder.buildPodsPath();
    // Document doc = new Document();
    // client.create().withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
    // .forPath(podsPath.toString(), DocumentSerializer.toByteArray(doc));
    // doc.setVersion(0);
    //
    // // create a pod with podId = 0
    // Path podPath = PathBuilder.buildPodPath("0");
    // doc = new Document();
    // client.create().withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
    // .forPath(podPath.toString(), DocumentSerializer.toByteArray(doc));
    // doc.setVersion(0);
    //
    // ConfigJson config = new ConfigJson();
    // config.setConnectionString(server.getConnectString());
    // config.setPodId("ignored");
    //
    // OutputStream stream = new ByteArrayOutputStream();
    //
    // new ObjectMapper().writeValue(stream, config);
    //
    // // try to start CamilleEnvironment in Bootstrap mode when a pod
    // // already exists
    // // this should fail
    // try {
    // CamilleEnvironment.start(Mode.BOOTSTRAP, new
    // StringReader(stream.toString()));
    // Assert.fail("A RuntimeException was expected.");
    // } catch (RuntimeException e) {
    // }
    // } finally {
    // CamilleEnvironment.stop();
    // }
    // }
    //
    // @Test(groups = "unit")
    // public synchronized void testBootstrapWithOnePod() throws Exception {
    // try (TestingServer server = new TestingServer()) {
    //
    // CuratorFramework client =
    // CuratorFrameworkFactory.newClient(server.getConnectString(),
    // new ExponentialBackoffRetry(1000, 3));
    // client.start();
    // try {
    // client.blockUntilConnected();
    // } catch (InterruptedException ie) {
    // Assert.fail("There was a problem with the testing server.");
    // }
    //
    // CamilleEnvironment.stop();
    //
    // ConfigJson config = new ConfigJson();
    // config.setConnectionString(server.getConnectString());
    // config.setPodId("ignored");
    //
    // OutputStream stream = new ByteArrayOutputStream();
    //
    // new ObjectMapper().writeValue(stream, config);
    //
    // CamilleEnvironment.start(Mode.BOOTSTRAP, new
    // StringReader(stream.toString()));
    // } finally {
    // CamilleEnvironment.stop();
    // }
    // }
    //
    // @Test(groups = "unit")
    // public synchronized void testRuntimeWithPod() throws Exception {
    // try (TestingServer server = new TestingServer()) {
    //
    // String podId = "pod0";
    //
    // CuratorFramework client =
    // CuratorFrameworkFactory.newClient(server.getConnectString(),
    // new ExponentialBackoffRetry(1000, 3));
    // client.start();
    // try {
    // client.blockUntilConnected();
    // } catch (InterruptedException ie) {
    // Assert.fail("There was a problem with the testing server.");
    // }
    //
    // CamilleEnvironment.stop();
    //
    // // create Pods path
    // Path podsPath = PathBuilder.buildPodsPath();
    // Document doc = new Document();
    // client.create().withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
    // .forPath(podsPath.toString(), DocumentSerializer.toByteArray(doc));
    // doc.setVersion(0);
    //
    // // create a pod
    // Path podPath = PathBuilder.buildPodPath(podId);
    // doc = new Document();
    // client.create().withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
    // .forPath(podPath.toString(), DocumentSerializer.toByteArray(doc));
    // doc.setVersion(0);
    //
    // ConfigJson config = new ConfigJson();
    // config.setConnectionString(server.getConnectString());
    // config.setPodId(podId);
    //
    // OutputStream stream = new ByteArrayOutputStream();
    //
    // new ObjectMapper().writeValue(stream, config);
    //
    // CamilleEnvironment.start(Mode.RUNTIME, new
    // StringReader(stream.toString()));
    // } finally {
    // CamilleEnvironment.stop();
    // }
    // }
    //
    // @Test(groups = "unit")
    // public synchronized void testRuntimeWithNoPod() throws Exception {
    // try (TestingServer server = new TestingServer()) {
    //
    // String podId = "pod0";
    //
    // ConfigJson config = new ConfigJson();
    // config.setConnectionString(server.getConnectString());
    // config.setPodId(podId);
    //
    // OutputStream stream = new ByteArrayOutputStream();
    //
    // new ObjectMapper().writeValue(stream, config);
    //
    // CamilleEnvironment.stop();
    //
    // try {
    // CamilleEnvironment.start(Mode.RUNTIME, new
    // StringReader(stream.toString()));
    // Assert.fail("A RuntimeException was expected.");
    // } catch (RuntimeException e) {
    // }
    // } finally {
    // CamilleEnvironment.stop();
    // }
    // }
}
