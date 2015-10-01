package com.latticeengines.camille;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleCache;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
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
        Path path = new Path("/foo");
        Camille camille = CamilleEnvironment.getCamille();

        Document document = new Document();
        document.setData("foo");
        camille.create(path, document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(camille.exists(new Path("/foo")));

        CamilleCache cache = new CamilleCache(path);
        Document cached = cache.get();
        Assert.assertEquals(cached, document);

        document.setData("bar");
        camille.set(path, document);
        cache.rebuild();

        cached = cache.get();
        Assert.assertEquals(cached, document);
    }

    @Test(groups = "unit")
    public void testCacheThrowsIfParentPathDoesntExist() throws Exception {
        Path path = new Path("/foo");
        CamilleCache cache = new CamilleCache(path);
        Document cached = cache.get();
        Assert.assertNull(cached);
        path = new Path("/foo/bar");
        boolean threw = false;
        try {
            cache = new CamilleCache(path); // throws
        } catch (Exception e) {
            threw = true;
        }
        Assert.assertTrue(threw);
    }
}
