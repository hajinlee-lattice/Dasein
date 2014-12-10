package com.latticeengines.camille;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.camille.exposed.CamilleTransaction;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class CamilleTransactionUnitTestNG {

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
    public void testVersionPopulatedOnCreate() throws Exception {
        Document document = new Document("foo");

        CamilleTransaction transaction = new CamilleTransaction();
        transaction.create(new Path("/foo"), document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        transaction.commit();

        Assert.assertEquals(document.getVersion(), 0);
    }

    @Test(groups = "unit")
    public void testVersionIncreasesOnSet() throws Exception {
        Document document = new Document("foo");

        CamilleTransaction transaction = new CamilleTransaction();
        transaction.create(new Path("/foo"), document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        transaction.commit();

        transaction = new CamilleTransaction();
        transaction.set(new Path("/foo"), document);
        transaction.commit();

        Assert.assertEquals(document.getVersion(), 1);

        transaction = new CamilleTransaction();
        transaction.set(new Path("/foo"), document);
        transaction.commit();

        Assert.assertEquals(document.getVersion(), 2);
    }

    // TODO more tests involving Camille to check whether documents exist,
    // etc...
}
