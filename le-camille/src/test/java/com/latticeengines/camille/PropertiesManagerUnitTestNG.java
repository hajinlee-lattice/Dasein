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

public class PropertiesManagerUnitTestNG {

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
    public void test() throws Exception {
        Path path = new Path("/foo");
        Camille camille = CamilleEnvironment.getCamille();

        Document document = new Document();
        camille.create(path, document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertTrue(camille.exists(path));

        PropertiesManager pm = new PropertiesManager(path);

        double d = 10;
        String dblName = "myDouble";

        int i = 2 ^ 8;
        String intName = "myInt";

        String s = new java.util.Date().toString();
        String strName = "myString";

        pm.setDoubleProperty(dblName, d);
        pm.setIntProperty(intName, i);
        pm.setStringProperty(strName, s);

        Assert.assertEquals(pm.getStringProperty(strName), s);
        Assert.assertEquals(pm.getDoubleProperty(dblName), d);
        Assert.assertEquals(pm.getIntProperty(intName), i);
    }
}
