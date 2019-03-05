package com.latticeengines.camille.config.cache;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.camille.exposed.config.cache.ConfigurationCache;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.camille.scopes.PodDivisionScope;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;
import com.latticeengines.domain.exposed.camille.scopes.TenantScope;

public class ConfigurationCacheUnitTestNG {

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
        Path path = new Path("/foo");

        ConfigurationController<PodScope> controller = ConfigurationController.construct(scope);
        controller.create(path, new Document("foo"));

        ConfigurationCache<PodScope> cache = ConfigurationCache.construct(scope, path);

        Assert.assertEquals(cache.get(), controller.get(path));
    }

    @Test(groups = "unit")
    public void testPodDivisionScope() throws Exception {
        PodDivisionScope scope = new PodDivisionScope();
        Path path = new Path("/foo");

        ConfigurationController<PodDivisionScope> controller = ConfigurationController.construct(scope);
        controller.create(path, new Document("foo"));

        ConfigurationCache<PodDivisionScope> cache = ConfigurationCache.construct(scope, path);

        Assert.assertEquals(cache.get(), controller.get(path));
    }

    @Test(groups = "unit")
    public void testContractScope() throws Exception {
        ContractScope scope = new ContractScope(CamilleTestEnvironment.getContractId());
        Path path = new Path("/foo");

        ConfigurationController<ContractScope> controller = ConfigurationController.construct(scope);
        controller.create(path, new Document("foo"));

        ConfigurationCache<ContractScope> cache = ConfigurationCache.construct(scope, path);

        Assert.assertEquals(cache.get(), controller.get(path));
    }

    @Test(groups = "unit")
    public void testTenantScope() throws Exception {
        TenantScope scope = new TenantScope(CamilleTestEnvironment.getContractId(),
                CamilleTestEnvironment.getTenantId());
        Path path = new Path("/foo");

        ConfigurationController<TenantScope> controller = ConfigurationController.construct(scope);
        controller.create(path, new Document("foo"));

        ConfigurationCache<TenantScope> cache = ConfigurationCache.construct(scope, path);

        Assert.assertEquals(cache.get(), controller.get(path));
    }

    @Test(groups = "unit")
    public void testSpaceScope() throws Exception {
        CustomerSpaceScope scope = new CustomerSpaceScope(CamilleTestEnvironment.getCustomerSpace());
        Path path = new Path("/foo");

        ConfigurationController<CustomerSpaceScope> controller = ConfigurationController.construct(scope);
        controller.create(path, new Document("foo"));

        ConfigurationCache<CustomerSpaceScope> cache = ConfigurationCache.construct(scope, path);

        Assert.assertEquals(cache.get(), controller.get(path));
    }
}
