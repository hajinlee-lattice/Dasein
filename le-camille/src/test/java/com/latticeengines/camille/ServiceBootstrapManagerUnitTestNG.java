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
import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.cache.ConfigurationCache;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState.State;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

public class ServiceBootstrapManagerUnitTestNG extends BaseBootstrapManagerUnitTestNG<ServiceScope> {
    @Override
    public ServiceScope getTestScope() {
        return new ServiceScope("MyService");
    }

    @Override
    public BootstrapState getState() throws Exception {
        return ServiceBootstrapManager.getBootstrapState(getTestScope().getServiceName());
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
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES, new Bootstrapper());

        // Bootstrap the initial configuration
        ServiceBootstrapManager.bootstrap(scope);

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    /**
     * Service scope doesn't support upgrades. Instead, services are responsible
     * for upgrading the data outside of the Camille framework, generally
     * through backwards compatibility. Breaking changes to the data break
     * rolling upgrades.
     */
    @Test(groups = "unit")
    public void testInstallNewVersion() throws Exception {
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES, new Bootstrapper());
        ServiceBootstrapManager.bootstrap(scope);

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));

        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), UPGRADED_VERSION_PROPERTIES, new Bootstrapper());
        ServiceBootstrapManager.bootstrap(scope);

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    @Test(groups = "unit", invocationCount = 10)
    public void testMultithreadedInstall() throws Exception {
        // Using multiple threads, bootstrap the initial configuration
        final ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES, new Bootstrapper());

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            for (int i = 0; i < 4; ++i) {
                exec.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ServiceBootstrapManager.bootstrap(scope);
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
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES, new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationController<ServiceScope> controller = ConfigurationController.construct(getTestScope());

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    @Test(groups = "unit")
    public void testConfigurationTransactionBootstraps() throws Exception {
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES, new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationTransaction<ServiceScope> transaction = ConfigurationTransaction.construct(getTestScope());

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    @Test(groups = "unit")
    public void testConfigurationCacheBootstraps() throws Exception {
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES, new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationCache<ServiceScope> cache = ConfigurationCache.construct(getTestScope(), new Path("/a"));

        Assert.assertTrue(serviceIsInState(State.OK, INITIAL_VERSION));
    }

    @Test(groups = "unit")
    public void testInstallFailure() throws Exception {
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), INITIAL_VERSION_PROPERTIES, new EvilBootstrapper());

        // Bootstrap the initial configuration
        boolean thrown = false;
        try {
            ServiceBootstrapManager.bootstrap(scope);
        } catch (Exception e) {
            thrown = true;
        }
        Assert.assertTrue(thrown, "Expected ServiceBootstrapManager to throw an exception");
        Assert.assertTrue(serviceIsInState(State.ERROR, INITIAL_VERSION, -1));
    }

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());
}
