package com.latticeengines.camille.config.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.config.ConfigurationController;
import com.latticeengines.camille.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.lifecycle.PodLifecycleManager;
import com.latticeengines.camille.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;
import com.latticeengines.domain.exposed.camille.scopes.TenantScope;

public class ConfigurationCacheUnitTestNG {

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
    public void testPodScope() throws Exception {
        PodScope scope = new PodScope();
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        Path path = new Path("/foo");

        ConfigurationController<PodScope> controller = ConfigurationController.construct(scope);
        controller.create(path, new Document("foo"));

        ConfigurationCache<PodScope> cache = ConfigurationCache.construct(scope, path);

        Assert.assertEquals(cache.get(), controller.get(path));
    }

    @Test(groups = "unit")
    public void testContractScope() throws Exception {
        ContractScope scope = new ContractScope("MyContract");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractId());
        Path path = new Path("/foo");

        ConfigurationController<ContractScope> controller = ConfigurationController.construct(scope);
        controller.create(path, new Document("foo"));

        ConfigurationCache<ContractScope> cache = ConfigurationCache.construct(scope, path);

        Assert.assertEquals(cache.get(), controller.get(path));
    }

    @Test(groups = "unit")
    public void testTenantScope() throws Exception {
        TenantScope scope = new TenantScope("MyContract", "MyTenant");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractId());
        TenantLifecycleManager.create(scope.getContractId(), scope.getTenantId(), "MySpace");
        Path path = new Path("/foo");

        ConfigurationController<TenantScope> controller = ConfigurationController.construct(scope);
        controller.create(path, new Document("foo"));

        ConfigurationCache<TenantScope> cache = ConfigurationCache.construct(scope, path);

        Assert.assertEquals(cache.get(), controller.get(path));
    }

    @Test(groups = "unit")
    public void testSpaceScope() throws Exception {
        CustomerSpaceScope scope = new CustomerSpaceScope("MyContract", "MyTenant", "MySpace");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractId());
        TenantLifecycleManager.create(scope.getContractId(), scope.getTenantId(), "DefaultSpace");
        SpaceLifecycleManager.create(scope.getContractId(), scope.getTenantId(), scope.getSpaceId());
        Path path = new Path("/foo");

        ConfigurationController<CustomerSpaceScope> controller = ConfigurationController.construct(scope);
        controller.create(path, new Document("foo"));

        ConfigurationCache<CustomerSpaceScope> cache = ConfigurationCache.construct(scope, path);

        Assert.assertEquals(cache.get(), controller.get(path));
    }
}
