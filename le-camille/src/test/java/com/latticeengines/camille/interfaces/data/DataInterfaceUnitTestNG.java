package com.latticeengines.camille.interfaces.data;

import org.apache.zookeeper.KeeperException;
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
    public void test() throws Exception {
        String interfaceName = "interfaceName";
        DataInterfacePublisher pub = new DataInterfacePublisher(interfaceName);
        DataInterfaceSubscriber sub = new DataInterfaceSubscriber(interfaceName);

        Path relativePath = new Path("/relativePath");
        Document doc = new Document("document");

        pub.publish(relativePath, doc);

        Assert.assertEquals(sub.get(relativePath).getData(), doc.getData());

        pub.remove(relativePath);
        try {
            sub.get(relativePath);
            Assert.fail(String.format("path %s should not exist", relativePath));
        } catch (KeeperException.NoNodeException e) {
        }
    }
}
