package com.latticeengines.camille.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.config.ConfigurationController;
import com.latticeengines.camille.config.ConfigurationTransaction;
import com.latticeengines.camille.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.lifecycle.PodLifecycleManager;
import com.latticeengines.camille.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.camille.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;
import com.latticeengines.domain.exposed.camille.scopes.TenantScope;

public class ConfigurationTransactionUnitTestNG {

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
    public void testPodTransaction() throws Exception {
        PodScope scope = new PodScope();
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        Path path = new Path("/foo");
        ConfigurationTransaction<PodScope> transaction = new ConfigurationTransaction<PodScope>(scope);
        transaction.create(path, new Document("foo"));
        transaction.commit();

        ConfigurationController<PodScope> controller = new ConfigurationController<PodScope>(scope);
        Assert.assertTrue(controller.exists(path));
    }

    @Test(groups = "unit")
    public void testContractTransaction() throws Exception {
        ContractScope scope = new ContractScope("MyContract");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractId());
        Path path = new Path("/foo");

        ConfigurationTransaction<ContractScope> transaction = new ConfigurationTransaction<ContractScope>(scope);
        transaction.create(path, new Document("foo"));
        transaction.commit();

        ConfigurationController<ContractScope> controller = new ConfigurationController<ContractScope>(scope);
        Assert.assertTrue(controller.exists(path));
    }

    @Test(groups = "unit")
    public void testTenantTransaction() throws Exception {
        TenantScope scope = new TenantScope("MyContract", "MyTenant");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractId());
        TenantLifecycleManager.create(scope.getContractId(), scope.getTenantId(), "MySpace");
        Path path = new Path("/foo");

        ConfigurationTransaction<TenantScope> transaction = new ConfigurationTransaction<TenantScope>(scope);
        transaction.create(path, new Document("foo"));
        transaction.commit();

        ConfigurationController<TenantScope> controller = new ConfigurationController<TenantScope>(scope);
        Assert.assertTrue(controller.exists(path));
    }

    /**
     * Test when specifying an explicit space that's different from the default
     * space
     */
    @Test(groups = "unit")
    public void testSpaceTransaction() throws Exception {
        CustomerSpaceScope scope = new CustomerSpaceScope("MyContract", "MyTenant", "MySpace");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractId());
        TenantLifecycleManager.create(scope.getContractId(), scope.getTenantId(), "DefaultSpace");
        SpaceLifecycleManager.create(scope.getContractId(), scope.getTenantId(), scope.getSpaceId());
        Path path = new Path("/foo");

        ConfigurationTransaction<CustomerSpaceScope> transaction = new ConfigurationTransaction<CustomerSpaceScope>(
                scope);
        transaction.create(path, new Document("foo"));
        transaction.commit();

        ConfigurationController<CustomerSpaceScope> controller = new ConfigurationController<CustomerSpaceScope>(scope);
        Assert.assertTrue(controller.exists(path));
    }

    /**
     * Test when using the default space
     */
    @Test(groups = "unit")
    public void testSpaceCreateDeleteDocumentUsingDefaultSpace() throws Exception {
        CustomerSpaceScope scope = new CustomerSpaceScope("MyContract", "MyTenant");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractId());
        TenantLifecycleManager.create(scope.getContractId(), scope.getTenantId(), "MySpace");
        SpaceLifecycleManager.create(scope.getContractId(), scope.getTenantId(), "MySpace");
        Path path = new Path("/foo");

        ConfigurationTransaction<CustomerSpaceScope> transaction = new ConfigurationTransaction<CustomerSpaceScope>(
                scope);
        transaction.create(path, new Document("foo"));
        transaction.commit();

        ConfigurationController<CustomerSpaceScope> controller = new ConfigurationController<CustomerSpaceScope>(scope);
        Assert.assertTrue(controller.exists(path));
    }
}
