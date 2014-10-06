package com.latticeengines.camille;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.config.ConfigurationController;
import com.latticeengines.camille.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.lifecycle.PodLifecycleManager;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;

public class ConfigurationControllerUnitTestNG {

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
    public void testPodCreateDeleteDocument() throws Exception {
        PodScope scope = new PodScope("MyPod");
        PodLifecycleManager.create(scope.getPodID());
        Path path = new Path("/foo");
        ConfigurationController<PodScope> controller = new ConfigurationController<PodScope>(scope);
        controller.create(path, new Document("foo"));
        Assert.assertTrue(controller.exists(path));
        controller.delete(path);
        Assert.assertFalse(controller.exists(path));
    }

    @Test(groups = "unit")
    public void testContractCreateDeleteDocument() throws Exception {
        ContractScope scope = new ContractScope("MyContract");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractID());
        Path path = new Path("/foo");
        ConfigurationController<ContractScope> controller = new ConfigurationController<ContractScope>(scope);
        controller.create(path, new Document("foo"));
        Assert.assertTrue(controller.exists(path));
        controller.delete(path);
        Assert.assertFalse(controller.exists(path));
    }
}
