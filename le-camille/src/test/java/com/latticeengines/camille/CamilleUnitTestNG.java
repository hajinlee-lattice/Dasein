package com.latticeengines.camille;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.StringReader;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.CamilleEnvironment.Mode;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.netflix.curator.test.TestingServer;

public class CamilleUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static TestingServer initTestServerAndCamille() throws Exception {
        try {
            TestingServer server = new TestingServer();

            CamilleEnvironment.stop();

            ConfigJson config = new ConfigJson();
            config.setConnectionString(server.getConnectString());
            config.setPodId("testPodId");

            OutputStream oStream = new ByteArrayOutputStream();

            new ObjectMapper().writeValue(oStream, config);

            CamilleEnvironment.start(Mode.RUNTIME, new StringReader(oStream.toString()));

            return server;
        } catch (Exception e) {
            log.error("Error starting Camille environment", e);
            throw e;
        }
    }

    @Test(groups = "unit")
    public void testCreateGetWatchAndDelete() throws Exception {
        try (TestingServer server = initTestServerAndCamille()) {
            Camille c = CamilleEnvironment.getCamille();

            Path path = new Path("/testPath");
            Document doc0 = new Document("testData0", null);

            c.create(path, doc0, ZooDefs.Ids.OPEN_ACL_UNSAFE);

            Assert.assertTrue(c.exists(path));
            Assert.assertFalse(c.exists(new Path("/testWrongPath")));

            final boolean[] watchEventFired = { false };
            CuratorWatcher watcher = new CuratorWatcher() {
                @Override
                public void process(WatchedEvent event) throws Exception {
                    if (event.getType().equals(Watcher.Event.EventType.NodeDataChanged)) {
                        watchEventFired[0] = true;
                    }
                }
            };

            Assert.assertEquals(c.get(path, watcher).getData(), doc0.getData());

            Document doc1 = new Document("testData1", null);
            c.set(path, doc1);

            Assert.assertTrue(watchEventFired[0]);

            Assert.assertEquals(c.get(path).getData(), doc1.getData());

            c.delete(path);

            Assert.assertFalse(c.exists(path));
        } finally {
            CamilleEnvironment.stop();
        }
    }
}
