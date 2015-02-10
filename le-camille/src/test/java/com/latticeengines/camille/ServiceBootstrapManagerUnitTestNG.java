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
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

public class ServiceBootstrapManagerUnitTestNG extends BaseBootstrapManagerUnitTestNG<ServiceScope> {
    @Override
    public ServiceScope getTestScope() {
        return new ServiceScope("MyService", 1);
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
        ServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper());

        // Bootstrap the initial configuration
        ServiceBootstrapManager.bootstrap(scope);

        // Assert configuration is correct
        ConfigurationController<ServiceScope> controller = ConfigurationController.construct(scope);
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    /**
     * Service scope doesn't support upgrades. Instead, the files will be
     * deleted and reinstalled with the latest version if there is a version
     * mismatch.
     */
    @Test(groups = "unit")
    public void testInstallNewVersion() throws Exception {
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper());
        scope.setDataVersion(1);
        ServiceBootstrapManager.bootstrap(scope);

        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper());
        scope.setDataVersion(2);
        ServiceBootstrapManager.bootstrap(scope);
    }

    @Test(groups = "unit", invocationCount = 10)
    public void testMultithreadedInstall() throws Exception {
        // Using multiple threads, bootstrap the initial configuration
        final ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper());

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

        // Assert configuration is in the correct structure
        ConfigurationController<ServiceScope> controller = ConfigurationController.construct(scope);
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @Test(groups = "unit")
    public void testConfigurationControllerBootstraps() throws Exception {
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper());

        ConfigurationController<ServiceScope> controller = ConfigurationController.construct(getTestScope());
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @Test(groups = "unit")
    public void testConfigurationTransactionBootstraps() throws Exception {
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationTransaction<ServiceScope> transaction = ConfigurationTransaction.construct(getTestScope());
        ConfigurationController<ServiceScope> controller = ConfigurationController.construct(getTestScope());
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @Test(groups = "unit")
    public void testConfigurationCacheBootstraps() throws Exception {
        ServiceScope scope = getTestScope();
        ServiceBootstrapManager.reset(scope.getServiceName());
        ServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper());

        @SuppressWarnings("unused")
        ConfigurationCache<ServiceScope> cache = ConfigurationCache.construct(getTestScope(), new Path("/a"));
        ConfigurationController<ServiceScope> controller = ConfigurationController.construct(getTestScope());
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());
}
