package com.latticeengines.admin.dynamicopts.impl;

import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class ZNodeProviderUnitTestNG {

    private final Path ZNODE_PATH = new Path("/provider");
    private final List<String> OPTIONS = Arrays.asList("option1", "option2");

    private Camille camille;
    private ZNodeProvider provider;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        camille = CamilleEnvironment.getCamille();
        camille.create(ZNODE_PATH, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        provider = new ZNodeProvider(ZNODE_PATH);
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testZNodeProvider() throws Exception {
        camille.set(ZNODE_PATH, new Document(JsonUtils.serialize(OPTIONS)));
        Assert.assertEquals(provider.getOptions().toArray(), OPTIONS.toArray());
    }

    @Test(groups = "unit")
    public void testNonexistingZNode() throws Exception {
        List<String> options = provider.getOptions();
        Assert.assertTrue(options.isEmpty(), "options for a non-existing znode should be empty list.");
    }

    @Test(groups = "unit")
    public void testMalformedZNode() throws Exception {
        camille.set(ZNODE_PATH, new Document("{\"Reason\":\"this is not a list\"}"));

        List<String> options = provider.getOptions();
        Assert.assertTrue(options.isEmpty(), "options for a malformed znode should be empty list.");
    }
}
