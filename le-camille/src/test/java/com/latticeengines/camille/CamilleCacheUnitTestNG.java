package com.latticeengines.camille;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class CamilleCacheUnitTestNG {

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
    public void testCache() throws Exception {
        CamilleCache cache = new CamilleCache();
        Camille camille = CamilleEnvironment.getCamille();
        Path path = new Path("/foo");
        Document document = new Document();
        document.setData("foo");
        camille.create(path, document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(camille.exists(new Path("/foo")));
        cache.add(path);
        
        Document cached = cache.get(path);
        Assert.assertEquals(cached, document);
        
        document.setData("bar");
        camille.set(path, document);
        cached = cache.get(path);
        Assert.assertEquals(cached, document);
    }
}
