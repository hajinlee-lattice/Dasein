package com.latticeengines.camille.interfaces.data;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.CamilleTestEnvironment;
import com.latticeengines.camille.lifecycle.PodLifecycleManager;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class DataInterfaceUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testGet() throws Exception {
        String interfaceName = "interfaceName";
        DataInterfacePublisher pub = new DataInterfacePublisher(interfaceName);
        DataInterfaceSubscriber sub = new DataInterfaceSubscriber(interfaceName);

        Path relativePath = new Path("/relativePath");
        Document doc = new Document("document");

        pub.publish(relativePath, doc);

        Assert.assertEquals(sub.get(relativePath).getData(), doc.getData());

        pub.remove(relativePath);
        Assert.assertNull(sub.get(relativePath));
    }

    @Test(groups = "unit")
    public void testGetChildren() throws Exception {
        String interfaceName = "interfaceName";
        DataInterfacePublisher pub = new DataInterfacePublisher(interfaceName);
        DataInterfaceSubscriber sub = new DataInterfaceSubscriber(interfaceName);

        String relativePath = "relativePath";

        Path relativePath1 = new Path(String.format("/%s/1", relativePath));
        Document doc1 = new Document("document1");
        pub.publish(relativePath1, doc1);

        Path relativePath2 = new Path(String.format("/%s/2", relativePath));
        Document doc2 = new Document("document2");
        pub.publish(relativePath2, doc2);

        Assert.assertTrue(sub.getChildren(new Path("/" + relativePath)).containsAll(
                Arrays.asList(Pair.of(doc1, relativePath1), Pair.of(doc2, relativePath2))));
    }
}
