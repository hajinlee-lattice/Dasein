package com.latticeengines.camille;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.config.ConfigurationController;
import com.latticeengines.camille.config.ConfigurationTransaction;
import com.latticeengines.camille.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.config.bootstrap.Installer;
import com.latticeengines.camille.config.bootstrap.Upgrader;
import com.latticeengines.camille.config.bootstrap.VersionMismatchException;
import com.latticeengines.camille.config.cache.ConfigurationCache;
import com.latticeengines.camille.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.paths.PathConstants;
import com.latticeengines.camille.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceBootstrapManagerUnitTestNG {
    private final static Semaphore semaphore = new Semaphore(1);

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        CustomerSpace space = getCustomerSpace();
        ContractLifecycleManager.create(space.getContractId());
        TenantLifecycleManager.create(space.getContractId(), space.getTenantId(), space.getSpaceId());
        semaphore.acquire();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        semaphore.release();
        CamilleTestEnvironment.stop();
    }

    private static CustomerSpace getCustomerSpace() {
        return new CustomerSpace("ContractID", "TenantID", "SpaceID");
    }

    private static CustomerSpaceServiceScope getCustomerSpaceServiceScope() {
        return new CustomerSpaceServiceScope(getCustomerSpace(), "MyService", 1);
    }

    private static class Bootstrapper implements Installer, Upgrader {

        @Override
        public DocumentDirectory upgradeConfiguration(int sourceVersion, int targetVersion, DocumentDirectory source) {
            return CustomerSpaceServiceBootstrapManagerUnitTestNG.getUpgradedConfiguration();
        }

        @Override
        public DocumentDirectory getInitialConfiguration(int dataVersion) {
            return CustomerSpaceServiceBootstrapManagerUnitTestNG.getInitialConfiguration();
        }
    }

    private static DocumentDirectory getInitialConfiguration() {
        final Integer version = 1;
        DocumentDirectory directory = new DocumentDirectory(new Path("/"));
        directory.add(new Path("/a"));
        directory.add(new Path("/a/b"), new Document(version.toString()));
        directory.add(new Path("/a/c"));
        directory.add(new Path("/a/c/d"), new Document(version.toString()));
        return directory;
    }

    private static DocumentDirectory getUpgradedConfiguration() {
        final Integer version = 2;
        DocumentDirectory directory = new DocumentDirectory(new Path("/"));
        directory.add(new Path("/a"));
        directory.add(new Path("/a/b"), new Document(version.toString()));
        directory.add(new Path("/a/d"), new Document(version.toString()));
        directory.add(new Path("/a/e"), new Document(version.toString()));
        return directory;
    }

    @Test(groups = "unit")
    public void testInstall() throws Exception {
        CustomerSpaceServiceScope scope = getCustomerSpaceServiceScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());

        // Bootstrap the initial configuration
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);

        // Assert configuration is correct
        ConfigurationController<CustomerSpaceServiceScope> controller = new ConfigurationController<CustomerSpaceServiceScope>(
                scope);
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));
        
        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @Test(groups = "unit")
    public void testUpgrade() throws Exception {
        CustomerSpaceServiceScope scope = getCustomerSpaceServiceScope();
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
        ConfigurationController<CustomerSpaceServiceScope> controller = new ConfigurationController<CustomerSpaceServiceScope>(
                scope);
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
        CustomerSpaceServiceScope scope = getCustomerSpaceServiceScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());
        scope.setDataVersion(5);
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
        final CustomerSpaceServiceScope scope = getCustomerSpaceServiceScope();
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
        ConfigurationController<CustomerSpaceServiceScope> controller = new ConfigurationController<CustomerSpaceServiceScope>(
                scope);
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }

    @Test(groups = "unit")
    public void testConfigurationControllerBootstraps() throws Exception {
        CustomerSpaceServiceScope scope = getCustomerSpaceServiceScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());
        
        ConfigurationController<CustomerSpaceServiceScope> controller = new ConfigurationController<CustomerSpaceServiceScope>(
                getCustomerSpaceServiceScope());
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }
    
    @Test(groups = "unit")
    public void testConfigurationTransactionBootstraps() throws Exception {
        CustomerSpaceServiceScope scope = getCustomerSpaceServiceScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());
        
        @SuppressWarnings("unused")
        ConfigurationTransaction<CustomerSpaceServiceScope> transaction = new ConfigurationTransaction<CustomerSpaceServiceScope>(
                getCustomerSpaceServiceScope());
        ConfigurationController<CustomerSpaceServiceScope> controller = new ConfigurationController<CustomerSpaceServiceScope>(
                getCustomerSpaceServiceScope());
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }
    
    
    @Test(groups = "unit")
    public void testConfigurationCacheBootstraps() throws Exception {
        CustomerSpaceServiceScope scope = getCustomerSpaceServiceScope();
        CustomerSpaceServiceBootstrapManager.reset(scope.getServiceName(), scope.getCustomerSpace());
        CustomerSpaceServiceBootstrapManager.register(scope.getServiceName(), new Bootstrapper(), new Bootstrapper());
        
        @SuppressWarnings("unused")
        ConfigurationCache<CustomerSpaceServiceScope> cache = new ConfigurationCache<CustomerSpaceServiceScope>(
                getCustomerSpaceServiceScope(), new Path("/a"));
        ConfigurationController<CustomerSpaceServiceScope> controller = new ConfigurationController<CustomerSpaceServiceScope>(
                getCustomerSpaceServiceScope());
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        Assert.assertTrue(configurationEquals(configuration, getInitialConfiguration()));
    }
    
    
    private boolean configurationEquals(DocumentDirectory configurationFromZK, DocumentDirectory sourceConfiguration) {
        // TODO Eventually will not be necessary once ConfigurationControllers
        // omit hidden files
        configurationFromZK.delete(new Path("/").append(PathConstants.SERVICE_DATA_VERSION_FILE));
        return configurationFromZK.equals(sourceConfiguration);
    }
}
