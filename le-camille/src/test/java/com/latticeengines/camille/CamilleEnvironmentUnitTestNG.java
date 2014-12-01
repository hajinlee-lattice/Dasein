package com.latticeengines.camille;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.StringReader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooDefs;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.CamilleEnvironment.Mode;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.netflix.curator.test.TestingServer;

public class CamilleEnvironmentUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

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

            ConfigJson config = new ConfigJson();
            config.setConnectionString(server.getConnectString());
            config.setPodId("ignored");

            OutputStream stream = new ByteArrayOutputStream();

            new ObjectMapper().writeValue(stream, config);

            CamilleEnvironment.start(Mode.BOOTSTRAP, new StringReader(stream.toString()));
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

            ConfigJson config = new ConfigJson();
            config.setConnectionString(server.getConnectString());
            config.setPodId(podId);

            OutputStream stream = new ByteArrayOutputStream();

            new ObjectMapper().writeValue(stream, config);

            CamilleEnvironment.start(Mode.RUNTIME, new StringReader(stream.toString()));
        } finally {
            CamilleEnvironment.stop();
        }
    }

    @Test(groups = "unit")
    public synchronized void testRuntimeWithNoPod() throws Exception {
        try (TestingServer server = new TestingServer()) {

            String podId = "pod0";

            ConfigJson config = new ConfigJson();
            config.setConnectionString(server.getConnectString());
            config.setPodId(podId);

            OutputStream stream = new ByteArrayOutputStream();

            new ObjectMapper().writeValue(stream, config);

            CamilleEnvironment.stop();

            try {
                CamilleEnvironment.start(Mode.RUNTIME, new StringReader(stream.toString()));
                Assert.fail("A RuntimeException was expected.");
            } catch (RuntimeException e) {
            }
        } finally {
            CamilleEnvironment.stop();
        }
    }
}
