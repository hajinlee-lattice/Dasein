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
import com.latticeengines.camille.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;
import com.latticeengines.domain.exposed.camille.scopes.TenantScope;

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
        PodScope scope = new PodScope();
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        Path path = new Path("/foo");
        ConfigurationController<PodScope> controller = new ConfigurationController<PodScope>(scope);
        controller.create(path, new Document("foo"));
        Assert.assertTrue(controller.exists(path));
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildPodPath(CamilleEnvironment.getPodId()).append(path)));
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
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), scope.getContractID()).append(path)));
        controller.delete(path);
        Assert.assertFalse(controller.exists(path));
    }

    @Test(groups = "unit")
    public void testTenantCreateDeleteDocument() throws Exception {
        TenantScope scope = new TenantScope("MyContract", "MyTenant");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractID());
        TenantLifecycleManager.create(scope.getContractID(), scope.getTenantID(), "MySpace");
        Path path = new Path("/foo");
        ConfigurationController<TenantScope> controller = new ConfigurationController<TenantScope>(scope);
        controller.create(path, new Document("foo"));
        Assert.assertTrue(controller.exists(path));
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), scope.getContractID(), scope.getTenantID())
                        .append(path)));
        controller.delete(path);
        Assert.assertFalse(controller.exists(path));
    }

    /**
     * Test when specifying an explicit space that's different from the default
     * space
     */
    @Test(groups = "unit")
    public void testSpaceCreateDeleteDocument() throws Exception {
        CustomerSpaceScope scope = new CustomerSpaceScope("MyContract", "MyTenant", "MySpace");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractID());
        TenantLifecycleManager.create(scope.getContractID(), scope.getTenantID(), "DefaultSpace");
        SpaceLifecycleManager.create(scope.getContractID(), scope.getTenantID(), scope.getSpaceID());
        Path path = new Path("/foo");
        ConfigurationController<CustomerSpaceScope> controller = new ConfigurationController<CustomerSpaceScope>(scope);
        controller.create(path, new Document("foo"));
        Assert.assertTrue(controller.exists(path));
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), scope.getContractID(),
                        scope.getTenantID(), scope.getSpaceID()).append(path)));
        controller.delete(path);
        Assert.assertFalse(controller.exists(path));
    }

    /**
     * Test when using the default space
     */
    @Test(groups = "unit")
    public void testSpaceCreateDeleteDocumentUsingDefaultSpace() throws Exception {
        CustomerSpaceScope scope = new CustomerSpaceScope("MyContract", "MyTenant");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractID());
        TenantLifecycleManager.create(scope.getContractID(), scope.getTenantID(), "MySpace");
        SpaceLifecycleManager.create(scope.getContractID(), scope.getTenantID(), "MySpace");
        Path path = new Path("/foo");
        ConfigurationController<CustomerSpaceScope> controller = new ConfigurationController<CustomerSpaceScope>(scope);
        controller.create(path, new Document("foo"));
        Assert.assertTrue(controller.exists(path));
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), scope.getContractID(),
                        scope.getTenantID(), "MySpace").append(path)));
        controller.delete(path);
        Assert.assertFalse(controller.exists(path));
    }
}
