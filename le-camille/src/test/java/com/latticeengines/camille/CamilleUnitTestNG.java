package com.latticeengines.camille;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.FileSystemGetChildrenFunction;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory.Node;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.VersionedDocument;

public class CamilleUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final int timeOutMs = 2000;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void createWithEmptyIntermediateNodes() throws Exception {
        Camille c = CamilleEnvironment.getCamille();
        Path fullPath = new Path("/0/1/2");
        c.create(fullPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        int i = 0;
        for (Path p : fullPath.getParentPaths()) {
            Assert.assertTrue(c.exists(p));
            ++i;
        }
        Assert.assertEquals(i, 2);
    }

    @Test(groups = "unit", expectedExceptions = Exception.class)
    public void createWithoutIntermediateDocs() throws Exception {
        Camille c = CamilleEnvironment.getCamille();
        Path fullPath = new Path("/0/1/2");
        c.create(fullPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testCreateGetSetWatchAndDelete() throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        Path path = new Path("/testPath");
        Document doc0 = new Document("testData0");

        c.create(path, doc0, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        Assert.assertEquals(doc0.getVersion(), 0);

        Assert.assertTrue(c.exists(path));
        Assert.assertFalse(c.exists(new Path("/testWrongPath")));

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

        Document doc1 = new Document("testData1");
        c.set(path, doc1);
        latch.await(); // wait for the process callback to be called
        Assert.assertTrue(dataChangedEventFired[0]);

        Assert.assertEquals(doc1.getVersion(), 1);

        Assert.assertEquals(c.get(path).getData(), doc1.getData());

        Assert.assertEquals(c.get(path).getVersion(), 1);

        c.delete(path);

        Assert.assertFalse(c.exists(path));
    }

    @Test(groups = "unit")
    public void testGetChildren() throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        Path parentPath = new Path("/parentPath");
        Document parentDoc = new Document("parentData");
        c.create(parentPath, parentDoc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(c.exists(parentPath));

        Path childPath0 = new Path(String.format("%s/%s", parentPath, "childPath0"));
        Document childDoc0 = new Document("child0Data");
        c.create(childPath0, childDoc0, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(c.exists(childPath0));

        Path childPath1 = new Path(String.format("%s/%s", parentPath, "childPath1"));
        Document childDoc1 = new Document("child1Data");
        c.create(childPath1, childDoc1, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(c.exists(childPath1));

        Set<AbstractMap.SimpleEntry<String, String>> actualChildren = new HashSet<>();
        for (AbstractMap.SimpleEntry<Document, Path> childPair : c.getChildren(parentPath)) {
            actualChildren.add(new AbstractMap.SimpleEntry<String, String>(childPair.getKey().getData(), childPair.getValue().toString()));
        }

        Assert.assertTrue(actualChildren.contains(new AbstractMap.SimpleEntry<String, String>(childDoc0.getData(), childPath0.toString())));
        Assert.assertTrue(actualChildren.contains(new AbstractMap.SimpleEntry<String, String>(childDoc1.getData(), childPath1.toString())));
    }

    @Test(groups = "unit")
    public void testDocumentDirectory() throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        Path p0 = new Path("/parentPath");
        Document d0 = new Document("d0");
        c.create(p0, d0, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p0));

        Path p1 = new Path(String.format("%s/%s", p0, "p1"));
        Document d1 = new Document("d1");
        c.create(p1, d1, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p1));

        Path p2 = new Path(String.format("%s/%s", p0, "p2"));
        Document d2 = new Document("d2");
        c.create(p2, d2, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p2));

        Path p3 = new Path(String.format("%s/%s", p1, "p3"));
        Document d3 = new Document("d3");
        c.create(p3, d3, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p3));

        Path p4 = new Path(String.format("%s/%s", p1, "p4"));
        Document d4 = new Document("d4");
        c.create(p4, d4, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p4));

        Path p5 = new Path(String.format("%s/%s", p2, "p5"));
        Document d5 = new Document("d5");
        c.create(p5, d5, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p5));

        Path p6 = new Path(String.format("%s/%s", p2, "p6"));
        Document d6 = new Document("d6");
        c.create(p6, d6, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p6));

        DocumentDirectory h = c.getDirectory(p0);

        Assert.assertEquals(h.getChildren().size(), 2);
        Assert.assertEquals(h.getChildren().get(0).getChildren().size(), 2);
        for (DocumentDirectory.Node node : h.getChildren().get(0).getChildren()) {
            Assert.assertTrue(node.getChildren().isEmpty());
        }
        Assert.assertEquals(h.getChildren().get(1).getChildren().size(), 2);
        for (DocumentDirectory.Node node : h.getChildren().get(1).getChildren()) {
            Assert.assertTrue(node.getChildren().isEmpty());
        }

        int i = 1;
        Iterator<DocumentDirectory.Node> iter = h.breadthFirstIterator();
        while (iter.hasNext()) {
            Assert.assertEquals(iter.next().getDocument().getData(), String.format("d%d", i));
            ++i;
        }
        Assert.assertEquals(i, 7);

        i = 1;
        iter = h.depthFirstIterator();
        while (iter.hasNext()) {
            switch (iter.next().getDocument().getData()) {
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
    }

    @Test(groups = "unit")
    public void testModifyDocumentDirectory() throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        Path p0 = new Path("/parentPath");
        Document d0 = new Document("d0");
        c.create(p0, d0, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p0));

        Path p1 = new Path(String.format("%s/%s", p0, "p1"));
        Document d1 = new Document("d1");
        c.create(p1, d1, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p1));

        Path p2 = new Path(String.format("%s/%s", p0, "p2"));
        Document d2 = new Document("d2");
        c.create(p2, d2, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p2));

        Path p3 = new Path(String.format("%s/%s", p1, "p3"));
        Document d3 = new Document("d3");
        c.create(p3, d3, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p3));

        Path p4 = new Path(String.format("%s/%s", p1, "p4"));
        Document d4 = new Document("d4");
        c.create(p4, d4, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p4));

        Path p5 = new Path(String.format("%s/%s", p2, "p5"));
        Document d5 = new Document("d5");
        c.create(p5, d5, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p5));

        Path p6 = new Path(String.format("%s/%s", p2, "p6"));
        Document d6 = new Document("d6");
        c.create(p6, d6, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p6));

        DocumentDirectory directory = c.getDirectory(p0);

        Document d7 = new Document("d7");
        directory.add(new Path("/parentPath/d7"), d7);

        DocumentDirectory.Node n = directory.get(new Path("/parentPath/d7"));
        Assert.assertEquals(d7, n.getDocument());

        directory.delete(new Path("/parentPath/d7"));
        n = directory.get(new Path("/parentPath/d7"));
        Assert.assertNull(n);

        // Test local paths
        directory.makePathsLocal();
        Assert.assertEquals(directory.getRootPath(), new Path("/"));

        p1 = p1.local(p0);
        p2 = p2.local(p0);
        p3 = p3.local(p0);
        p4 = p4.local(p0);
        p5 = p5.local(p0);
        p6 = p6.local(p0);

        Assert.assertNotNull(directory.get(p1));
        Assert.assertNotNull(directory.get(p3));

        // Test cascade deletes
        directory.delete(p1);
        Assert.assertNull(directory.get(p1));
        Assert.assertNull(directory.get(p3));
        Assert.assertNull(directory.get(p4));
    }

    @Test(groups = "unit")
    public void testDocumentDirectoryOnRootPaths() throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        Path p0 = new Path("/parentPath");
        Document d0 = new Document("d0");
        c.create(p0, d0, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p0));

        Path p1 = new Path(String.format("%s/%s", p0, "p1"));
        Document d1 = new Document("d1");
        c.create(p1, d1, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p1));

        Path p2 = new Path(String.format("%s/%s", p0, "p2"));
        Document d2 = new Document("d2");
        c.create(p2, d2, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p2));

        Path p3 = new Path(String.format("%s/%s", p1, "p3"));
        Document d3 = new Document("d3");
        c.create(p3, d3, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p3));

        Path p4 = new Path(String.format("%s/%s", p1, "p4"));
        Document d4 = new Document("d4");
        c.create(p4, d4, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p4));

        Path p5 = new Path(String.format("%s/%s", p2, "p5"));
        Document d5 = new Document("d5");
        c.create(p5, d5, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p5));

        Path p6 = new Path(String.format("%s/%s", p2, "p6"));
        Document d6 = new Document("d6");
        c.create(p6, d6, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertNotNull(c.exists(p6));

        DocumentDirectory directory = c.getDirectory(new Path("/"));
        Assert.assertEquals(directory.getRootPath(), new Path("/"));
        directory.makePathsLocal();
        Assert.assertEquals(directory.getRootPath(), new Path("/"));

        Assert.assertNotNull(directory.get(p0));
        Assert.assertNotNull(directory.get(p3));
    }

    @Test(groups = "unit")
    public void testUpsert() throws Exception {
        Camille c = CamilleEnvironment.getCamille();
        Path path = new Path("/foo/bar/baz");
        c.upsert(path, new Document("foo"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        c.upsert(path, new Document("foo"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(c.exists(path));
    }

    @Test(groups = "unit")
    public void testCreateDirectory() throws IllegalArgumentException, Exception {
        File tempDir = Files.createTempDir();

        createDirectory(tempDir + "/0");
        createDirectory(tempDir + "/0/1");
        createDirectory(tempDir + "/0/2");
        createDirectory(tempDir + "/0/1/3");
        createDirectory(tempDir + "/0/1/4");
        createDirectory(tempDir + "/0/2/5");
        createDirectory(tempDir + "/0/2/6");

        createTextFile(tempDir + "/0/0.txt", "zero");
        createTextFile(tempDir + "/0/1/1.txt", "one");
        createTextFile(tempDir + "/0/2/2.txt", "two");
        createTextFile(tempDir + "/0/1/3/3.txt", "three");
        createTextFile(tempDir + "/0/1/4/4.txt", "four");
        createTextFile(tempDir + "/0/2/5/5.txt", "five");
        createTextFile(tempDir + "/0/2/6/6.txt", "six");

        DocumentDirectory docDir = new DocumentDirectory(new Path("/"), new FileSystemGetChildrenFunction(tempDir));

        Camille c = CamilleEnvironment.getCamille();

        Path parent = new Path("/parent");
        c.create(parent, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        c.createDirectory(parent, docDir, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        Iterator<Node> iter = c.getDirectory(parent).depthFirstIterator();

        for (String expected : new String[] { "/0", "/0/0.txt", "/0/1", "/0/1/1.txt", "/0/1/3", "/0/1/3/3.txt",
                "/0/1/4", "/0/1/4/4.txt", "/0/2", "/0/2/2.txt", "/0/2/5", "/0/2/5/5.txt", "/0/2/6", "/0/2/6/6.txt" }) {
            Node n = iter.next();
            Assert.assertEquals(n.getPath().toString(), parent.append(new Path(expected)).toString());

            if (StringUtils.endsWith(n.getPath().toString(), ".txt")) {
                switch (n.getPath().toString().substring(parent.toString().length())) {
                case "/0/0.txt":
                    Assert.assertEquals("zero", n.getDocument().getData());
                    break;
                case "/0/1/1.txt":
                    Assert.assertEquals("one", n.getDocument().getData());
                    break;
                case "/0/2/2.txt":
                    Assert.assertEquals("two", n.getDocument().getData());
                    break;
                case "/0/1/3/3.txt":
                    Assert.assertEquals("three", n.getDocument().getData());
                    break;
                case "/0/1/4/4.txt":
                    Assert.assertEquals("four", n.getDocument().getData());
                    break;
                case "/0/2/5/5.txt":
                    Assert.assertEquals("five", n.getDocument().getData());
                    break;
                case "/0/2/6/6.txt":
                    Assert.assertEquals("six", n.getDocument().getData());
                    break;
                default:
                    Assert.fail("We should never get here.");
                }
            }
        }

        FileUtils.deleteDirectory(tempDir);
    }

    @Test(groups = "unit")
    public void testUpsertDirectory() throws IllegalArgumentException, Exception {
        Camille c = CamilleEnvironment.getCamille();

        File tempDir = Files.createTempDir();
        Path parent = new Path("/parent");
        c.create(parent, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        // create directory + files. Make sure they exist.
        createDirectory(tempDir + "/0");
        createDirectory(tempDir + "/0/1");
        createTextFile(tempDir + "/0/0.txt", "zero");
        createTextFile(tempDir + "/0/1/1.txt", "one");

        DocumentDirectory docDir = new DocumentDirectory(new Path("/"), new FileSystemGetChildrenFunction(tempDir));
        c.upsertDirectory(parent, docDir, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(c.get(new Path("/parent/0/0.txt")).getData().equals("zero"));
        Assert.assertTrue(c.get(new Path("/parent/0/1/1.txt")).getData().equals("one"));

        // re-create one of the files with a new value. Make sure only the
        // changed file was affected.
        createTextFile(tempDir + "/0/1/1.txt", "two");
        docDir = new DocumentDirectory(new Path("/"), new FileSystemGetChildrenFunction(tempDir));
        c.upsertDirectory(parent, docDir, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(c.get(new Path("/parent/0/0.txt")).getData().equals("zero"));
        Assert.assertTrue(c.get(new Path("/parent/0/1/1.txt")).getData().equals("two"));
        FileUtils.deleteDirectory(tempDir);

        // delete the temp dir and create a new one with the same structure but
        // missing a file.
        // make sure the missing file is still in Camille.
        tempDir = Files.createTempDir();
        createDirectory(tempDir + "/0");
        createDirectory(tempDir + "/0/1");
        createTextFile(tempDir + "/0/1/1.txt", "three");
        docDir = new DocumentDirectory(new Path("/"), new FileSystemGetChildrenFunction(tempDir));
        c.upsertDirectory(parent, docDir, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(c.get(new Path("/parent/0/0.txt")).getData().equals("zero"));
        Assert.assertTrue(c.get(new Path("/parent/0/1/1.txt")).getData().equals("three"));
        FileUtils.deleteDirectory(tempDir);
    }

    public static class MyDocument extends VersionedDocument {
        public String foo;
    }

    @Test(groups = "unit")
    public void testVersionedDocument() throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        MyDocument typesafe = new MyDocument();
        typesafe.foo = "Foo";
        Document raw = DocumentUtils.toRawDocument(typesafe);
        // We should not be serializing out the document version
        Assert.assertFalse(raw.getData().contains("version"));
        Assert.assertFalse(raw.getData().contains("documentVersion"));
        c.create(new Path("/test"), raw, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Document rawRetrieved = c.get(new Path("/test"));
        MyDocument typesafeRetrieved = DocumentUtils.toTypesafeDocument(rawRetrieved, MyDocument.class);
        Assert.assertTrue(typesafeRetrieved.documentVersionSpecified());
        Assert.assertEquals(typesafeRetrieved.getDocumentVersion(), 0);
    }

    private static void createDirectory(String path) {
        File dir = new File(path);
        dir.mkdir();
        dir.deleteOnExit();
    }

    private static void createTextFile(String path, String contents) throws FileNotFoundException {
        try (PrintWriter w = new PrintWriter(path)) {
            w.print(contents);
        }
        new File(path).deleteOnExit();
    }
}
