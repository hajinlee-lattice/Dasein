package com.latticeengines.camille;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.ZooDefs;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentHierarchyCollection;
import com.latticeengines.domain.exposed.camille.DocumentHierarchyCollection.Node;
import com.latticeengines.domain.exposed.camille.DocumentMetadata;
import com.latticeengines.domain.exposed.camille.Path;

public class DocumentUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testClone() throws Exception {

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

        DocumentHierarchyCollection h = c.getDescendants(p0);

        DocumentHierarchyCollection hCopy = SerializationUtils.clone(h);
        Assert.assertFalse(h == hCopy);
        Assert.assertTrue(h.equals(hCopy));

        String data = "foo";
        DocumentMetadata meta = new DocumentMetadata();
        Document doc = new Document(data, meta);
        Document docCopy = SerializationUtils.clone(doc);

        Path p0Copy = SerializationUtils.clone(p0);
        Node n = h.breadthFirstIterator().next();
        Node nCopy = SerializationUtils.clone(n);

        Assert.assertFalse(n == nCopy);
        Assert.assertTrue(n.equals(nCopy));

        Assert.assertFalse(p0 == p0Copy);
        Assert.assertTrue(p0.equals(p0Copy));

        Assert.assertFalse(doc == docCopy);
        Assert.assertTrue(doc.equals(docCopy));

        Assert.assertFalse(docCopy.getData() == data);
        Assert.assertTrue(docCopy.getData().equals(data));

        Assert.assertFalse(docCopy.getMetadata() == meta);
        Assert.assertTrue(docCopy.getMetadata().equals(meta));

        Assert.assertTrue(docCopy.getVersion() == docCopy.getVersion());
    }
}
