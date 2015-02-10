package com.latticeengines.camille;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.camille.exposed.config.ConfigurationTransaction;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.VersionMismatchException;
import com.latticeengines.camille.exposed.config.cache.ConfigurationCache;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceBootstrapManagerUnitTestNG extends
        BaseBootstrapManagerUnitTestNG<CustomerSpaceServiceScope> {
    @Override
    public CustomerSpaceServiceScope getTestScope() {
        return new CustomerSpaceServiceScope(new CustomerSpace("ContractID", "TenantID", "SpaceID"), "MyService", 1);
    }

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        CustomerSpace space = getTestScope().getCustomerSpace();
        ContractLifecycleManager.create(space.getContractId());
        TenantLifecycleManager.create(space.getContractId(), space.getTenantId(), space.getSpaceId());
        lock();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        unlock();
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testInstall() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());

        // Bootstrap the initial configuration
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        // Assert configuration is correct
        ConfigurationController<CustomerSpaceServiceScope> controller = ConfigurationController.construct(scope);
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @Test(groups = "unit")
    public void testUpgrade() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());

        // Bootstrap the initial configuration
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        // Bootstrap again with an incremented data version to run the upgrade
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());

        scope.setDataVersion(2);
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        // Assert configuration is in the correct structure
        ConfigurationController<CustomerSpaceServiceScope> controller = ConfigurationController.construct(scope);
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getUpgradedConfiguration()));
    }

    /**
     * Tests the scenario where the data is being run on an earlier version of
     * the software
     * 
     * @throws Exception
     */
    @Test(groups = "unit", expectedExceptions = VersionMismatchException.class)
    public void testVersionMismatch() throws Exception {
        // Install a particular version
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());
        scope.setDataVersion(2);
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        // Claim to be version 1
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());
        scope.setDataVersion(1);
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);
    }

    @Test(groups = "unit", invocationCount = 10)
    public void testMultithreadedInstall() throws Exception {
        // Using multiple threads, bootstrap the initial configuration
        final CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            for (int i = 0; i < 4; ++i) {
                exec.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            CustomerSpaceServiceBootstrapManager.bootstrap(scope);
                        } catch (Exception e) {
                            Assert.fail("Unexpected failure", e);
                        }
                    }
                });
            }
        } finally {
            exec.shutdown();
            exec.awaitTermination(10, TimeUnit.SECONDS);
        }

        // Assert configuration is in the correct structure
        ConfigurationController<CustomerSpaceServiceScope> controller = ConfigurationController.construct(scope);
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @Test(groups = "unit")
    public void testConfigurationControllerBootstraps() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());

        ConfigurationController<CustomerSpaceServiceScope> controller = ConfigurationController
                .construct(getTestScope());
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @Test(groups = "unit")
    public void testConfigurationTransactionBootstraps() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationTransaction<CustomerSpaceServiceScope> transaction = ConfigurationTransaction
                .construct(getTestScope());
        ConfigurationController<CustomerSpaceServiceScope> controller = ConfigurationController
                .construct(getTestScope());
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @Test(groups = "unit")
    public void testConfigurationCacheBootstraps() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationCache<CustomerSpaceServiceScope> cache = ConfigurationCache.construct(getTestScope(), new Path(
                "/a"));
        ConfigurationController<CustomerSpaceServiceScope> controller = ConfigurationController
                .construct(getTestScope());
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());
}
