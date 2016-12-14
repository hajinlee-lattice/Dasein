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
import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.camille.exposed.config.bootstrap.VersionMismatchException;
import com.latticeengines.camille.exposed.config.cache.ConfigurationCache;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState.State;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceBootstrapManagerUnitTestNG extends
        BaseBootstrapManagerUnitTestNG<CustomerSpaceServiceScope> {
    @Override
    public CustomerSpaceServiceScope getTestScope() {
        return new CustomerSpaceServiceScope(CamilleTestEnvironment.getCustomerSpace(), "MyService");
    }

    @Override
    public BootstrapState getState() throws Exception {
        return CustomerSpaceServiceBootstrapManager.getBootstrapState(getTestScope().getServiceName(), getTestScope()
                .getCustomerSpace());
    }

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
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
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());

        // Bootstrap the initial configuration
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    @Test(groups = "unit")
    public void testUpgrade() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());

        // Bootstrap the initial configuration
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));

        // Bootstrap again with an incremented data version to run the upgrade
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), UPGRADED_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        Assert.assertTrue(serviceIsInState(State.OK, UPGRADED_VERSION));
    }

    @Test(groups = "unit")
    public void testNullSafe() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES, null, null,
                null);

        // Bootstrap the initial configuration
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        BootstrapState retrieved = getState();

        Assert.assertEquals(State.OK, retrieved.state);
        Assert.assertEquals(INITIAL_VERSION, retrieved.installedVersion);

        // Bootstrap again with an incremented data version to run the upgrade
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), UPGRADED_VERSION_PROPERTIES, null,
                null, null);
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        retrieved = getState();
        
        Assert.assertEquals(State.OK, retrieved.state);
        Assert.assertEquals(UPGRADED_VERSION, retrieved.installedVersion);
    }

    /**
     * Tests the scenario where the data is being run on an earlier version of
     * the software
     * 
     * @throws Exception
     */
    @Test(groups = "unit")
    public void testVersionMismatch() throws Exception {
        // Install a particular version
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), UPGRADED_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);
        Assert.assertTrue(serviceIsInState(State.OK, UPGRADED_VERSION));

        // Claim to be INITIAL_VERSION
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());

        boolean thrown = false;
        try {
            CustomerSpaceServiceBootstrapManager.bootstrap(scope);
        } catch (VersionMismatchException e) {
            thrown = true;
        }
        Assert.assertTrue(thrown, "VersionMismatchException not thrown");

        Assert.assertTrue(serviceIsInState(State.ERROR, INITIAL_VERSION, UPGRADED_VERSION));
    }

    @Test(groups = "unit", invocationCount = 10)
    public void testMultithreadedInstall() throws Exception {
        // Using multiple threads, bootstrap the initial configuration
        final CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());

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

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    @Test(groups = "unit")
    public void testConfigurationControllerBootstraps() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationController<CustomerSpaceServiceScope> controller = ConfigurationController
                .construct(getTestScope());

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    @Test(groups = "unit")
    public void testConfigurationTransactionBootstraps() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationTransaction<CustomerSpaceServiceScope> transaction = ConfigurationTransaction
                .construct(getTestScope());

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    @Test(groups = "unit")
    public void testConfigurationCacheBootstraps() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationCache<CustomerSpaceServiceScope> cache = ConfigurationCache.construct(getTestScope(), new Path(
                "/a"));

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    @Test(groups = "unit")
    public void testInstallFailure() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES,
                new EvilBootstrapper(), new EvilBootstrapper(), new EvilBootstrapper());

        // Bootstrap the initial configuration
        boolean thrown = false;
        try {
            CustomerSpaceServiceBootstrapManager.bootstrap(scope);
        } catch (Exception e) {
            thrown = true;
        }
        Assert.assertTrue(thrown, "Expected CustomerSpaceServiceBootstrapManager to throw an exception");
        Assert.assertTrue(serviceIsInState(State.ERROR, INITIAL_VERSION, -1));
    }

    @Test(groups = "unit")
    public void testUpgradeFailure() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper());

        // Bootstrap the initial configuration
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));

        // Bootstrap again with an incremented data version to run the upgrade
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), UPGRADED_VERSION_PROPERTIES,
                new EvilBootstrapper(), new EvilBootstrapper(), new EvilBootstrapper());

        boolean thrown = false;
        try {
            CustomerSpaceServiceBootstrapManager.bootstrap(scope);
        } catch (Exception e) {
            thrown = true;
        }
        Assert.assertTrue(thrown, "Expected CustomerSpaceServiceBootstrapManager to throw an exception");
        Assert.assertTrue(serviceIsInState(State.ERROR, UPGRADED_VERSION, INITIAL_VERSION));
    }

    /**
     * TODO Eventually move somewhere else...
     */
    @Test(groups = "unit", timeOut = 30000)
    public void testServiceWarden() throws Exception {
        CustomerSpaceServiceScope scope = getTestScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        ServiceBootstrapManager.reset(scope.getServiceName());

        ServiceWarden.registerService(scope.getServiceName(), new ServiceInfo(INITIAL_VERSION_PROPERTIES,
                new Bootstrapper(), new Bootstrapper(), new Bootstrapper(), new Bootstrapper()));
        ServiceWarden.commandBootstrap(scope.getServiceName(), scope.getCustomerSpace(), null);

        while (true) {
            BootstrapState state = CustomerSpaceServiceBootstrapManager.getBootstrapState(scope.getServiceName(),
                    scope.getCustomerSpace());
            if (state.installedVersion == INITIAL_VERSION) {
                break;
            }
            Assert.assertNotEquals(state.state, BootstrapState.State.ERROR);
            Thread.sleep(500);
        }
    }

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());
}
