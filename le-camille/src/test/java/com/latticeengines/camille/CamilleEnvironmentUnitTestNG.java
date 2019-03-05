package com.latticeengines.camille;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.netflix.curator.test.TestingServer;

public class CamilleEnvironmentUnitTestNG {

    @Test(groups = "unit")
    public synchronized void testBootstrapWithOnePod() throws Exception {
        try (TestingServer server = new TestingServer()) {

            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
                    new ExponentialBackoffRetry(1000, 3));
            client.start();
            try {
                client.blockUntilConnected();
            } catch (InterruptedException ie) {
                Assert.fail("There was a problem with the testing server.");
            }

            CamilleEnvironment.stop();

            CamilleConfiguration config = new CamilleConfiguration();
            config.setConnectionString(server.getConnectString());
            config.setPodId("ignored");

            CamilleEnvironment.start(Mode.BOOTSTRAP, config);
        } finally {
            CamilleEnvironment.stop();
        }
    }

    @Test(groups = "unit")
    public synchronized void testRuntimeWithPod() throws Exception {
        try (TestingServer server = new TestingServer()) {

            String podId = "pod0";

            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
                    new ExponentialBackoffRetry(1000, 3));
            client.start();
            try {
                client.blockUntilConnected();
            } catch (InterruptedException ie) {
                Assert.fail("There was a problem with the testing server.");
            }

            CamilleEnvironment.stop();

            // create Pods path
            Path podsPath = PathBuilder.buildPodsPath();
            Document doc = new Document();
            client.create().withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(podsPath.toString(), doc.getData().getBytes());
            doc.setVersion(0);

            // create a pod
            Path podPath = PathBuilder.buildPodPath(podId);
            doc = new Document();
            client.create().withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(podPath.toString(), doc.getData().getBytes());
            doc.setVersion(0);

            CamilleConfiguration config = new CamilleConfiguration();
            config.setConnectionString(server.getConnectString());
            config.setPodId(podId);

            CamilleEnvironment.start(Mode.RUNTIME, config);
        } finally {
            CamilleEnvironment.stop();
        }
    }

    @Test(groups = "unit")
    public synchronized void testRuntimeWithNoPod() throws Exception {
        try (TestingServer server = new TestingServer()) {

            String podId = "pod0";

            CamilleConfiguration config = new CamilleConfiguration();
            config.setConnectionString(server.getConnectString());
            config.setPodId(podId);

            CamilleEnvironment.stop();

            try {
                CamilleEnvironment.start(Mode.RUNTIME, config);
                Assert.fail("A RuntimeException was expected.");
            } catch (RuntimeException e) {
            }
        } finally {
            CamilleEnvironment.stop();
        }
    }
}
