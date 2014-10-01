package com.latticeengines.camille;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.tuple.Pair;
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
import com.latticeengines.domain.exposed.camille.DocumentHierarchy;
import com.latticeengines.domain.exposed.camille.DocumentHierarchy.Node;
import com.latticeengines.domain.exposed.camille.Path;
import com.netflix.curator.test.TestingServer;

public class CamilleUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final int timeOutMs = 2000;

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

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testCreateGetWatchAndDelete() throws Exception {
        try (TestingServer server = initTestServerAndCamille()) {
            Camille c = CamilleEnvironment.getCamille();

            Path path = new Path("/testPath");
            Document doc0 = new Document("testData0", null);

            c.create(path, doc0, ZooDefs.Ids.OPEN_ACL_UNSAFE);

            Assert.assertNotNull(c.exists(path));
            Assert.assertNull(c.exists(new Path("/testWrongPath")));

            // we need a CountDownLatch because the callback is called from
            // another thread
            final CountDownLatch latch = new CountDownLatch(1);

            final boolean[] dataChangedEventFired = { false };
            CuratorWatcher watcher = new CuratorWatcher() {
                @Override
                public void process(WatchedEvent event) throws Exception {
                    if (event.getType().equals(Watcher.Event.EventType.NodeDataChanged)) {
                        dataChangedEventFired[0] = true;
                    }
                    latch.countDown();
                }
            };

            Assert.assertEquals(c.get(path, watcher).getData(), doc0.getData());

            Document doc1 = new Document("testData1", null);
            c.set(path, doc1);
            latch.await(); // wait for the process callback to be called
            Assert.assertTrue(dataChangedEventFired[0]);

            Assert.assertEquals(c.get(path).getData(), doc1.getData());

            c.delete(path);

            Assert.assertNull(c.exists(path));
        } finally {
            CamilleEnvironment.stop();
        }
    }

    @Test(groups = "unit")
    public void testGetChildren() throws Exception {
        try (TestingServer server = initTestServerAndCamille()) {
            Camille c = CamilleEnvironment.getCamille();

            Path parentPath = new Path("/parentPath");
            Document parentDoc = new Document("parentData", null);
            c.create(parentPath, parentDoc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(parentPath));

            Path childPath0 = new Path(String.format("%s/%s", parentPath, "childPath0"));
            Document childDoc0 = new Document("child0Data", null);
            c.create(childPath0, childDoc0, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(childPath0));

            Path childPath1 = new Path(String.format("%s/%s", parentPath, "childPath1"));
            Document childDoc1 = new Document("child1Data", null);
            c.create(childPath1, childDoc1, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(childPath1));

            Set<Pair<String, String>> actualChildren = new HashSet<Pair<String, String>>();
            for (Pair<Document, Path> childPair : c.getChildren(parentPath)) {
                actualChildren.add(Pair.of(childPair.getLeft().getData(), childPair.getRight().toString()));
            }

            Assert.assertTrue(actualChildren.contains(Pair.of(childDoc0.getData(), childPath0.toString())));
            Assert.assertTrue(actualChildren.contains(Pair.of(childDoc1.getData(), childPath1.toString())));
        } finally {
            CamilleEnvironment.stop();
        }
    }

    @Test(groups = "unit")
    public void testDocumentHierarchy() throws Exception {
        try (TestingServer server = initTestServerAndCamille()) {
            Camille c = CamilleEnvironment.getCamille();

            Path p0 = new Path("/parentPath");
            Document d0 = new Document("d0", null);
            c.create(p0, d0, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(p0));

            Path p1 = new Path(String.format("%s/%s", p0, "p1"));
            Document d1 = new Document("d1", null);
            c.create(p1, d1, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(p1));

            Path p2 = new Path(String.format("%s/%s", p0, "p2"));
            Document d2 = new Document("d2", null);
            c.create(p2, d2, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(p2));

            Path p3 = new Path(String.format("%s/%s", p1, "p3"));
            Document d3 = new Document("d3", null);
            c.create(p3, d3, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(p3));

            Path p4 = new Path(String.format("%s/%s", p1, "p4"));
            Document d4 = new Document("d4", null);
            c.create(p4, d4, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(p4));

            Path p5 = new Path(String.format("%s/%s", p2, "p5"));
            Document d5 = new Document("d5", null);
            c.create(p5, d5, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(p5));

            Path p6 = new Path(String.format("%s/%s", p2, "p6"));
            Document d6 = new Document("d6", null);
            c.create(p6, d6, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            Assert.assertNotNull(c.exists(p6));

            DocumentHierarchy h = c.getHierarchy(p0);

            Assert.assertEquals(h.getRoot().getChildren().size(), 2);
            Assert.assertEquals(h.getRoot().getChildren().get(0).getChildren().size(), 2);
            for (Node node : h.getRoot().getChildren().get(0).getChildren()) {
                Assert.assertTrue(node.getChildren().isEmpty());
            }
            Assert.assertEquals(h.getRoot().getChildren().get(1).getChildren().size(), 2);
            for (Node node : h.getRoot().getChildren().get(1).getChildren()) {
                Assert.assertTrue(node.getChildren().isEmpty());
            }

            int i = 0;
            Iterator<Node> iter = h.breadthFirstIterator();
            while (iter.hasNext()) {
                Assert.assertEquals(iter.next().getDocument().getData(), String.format("d%d", i));
                ++i;
            }
            Assert.assertEquals(i, 7);

            i = 0;
            iter = h.depthFirstIterator();
            while (iter.hasNext()) {
                switch (iter.next().getDocument().getData()) {
                case "d0":
                    Assert.assertEquals(i, 0);
                    break;
                case "d1":
                    Assert.assertEquals(i, 1);
                    break;
                case "d2":
                    Assert.assertEquals(i, 4);
                    break;
                case "d3":
                    Assert.assertEquals(i, 2);
                    break;
                case "d4":
                    Assert.assertEquals(i, 3);
                    break;
                case "d5":
                    Assert.assertEquals(i, 5);
                    break;
                case "d6":
                    Assert.assertEquals(i, 6);
                    break;
                }

                ++i;
            }
            Assert.assertEquals(i, 7);

        } finally {
            CamilleEnvironment.stop();
        }
    }
}
