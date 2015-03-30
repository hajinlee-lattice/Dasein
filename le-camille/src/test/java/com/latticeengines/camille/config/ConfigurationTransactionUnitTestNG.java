package com.latticeengines.camille.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.camille.exposed.config.ConfigurationTransaction;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;
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
        Path path = new Path("/foo");
        ConfigurationTransaction<PodScope> transaction = ConfigurationTransaction.construct(scope);
        transaction.create(path, new Document("foo"));
        transaction.commit();

        ConfigurationController<PodScope> controller = ConfigurationController.construct(scope);
        Assert.assertTrue(controller.exists(path));
    }

    @Test(groups = "unit")
    public void testContractTransaction() throws Exception {
        ContractScope scope = new ContractScope(CamilleTestEnvironment.getContractId());
        Path path = new Path("/foo");

        ConfigurationTransaction<ContractScope> transaction = ConfigurationTransaction.construct(scope);
        transaction.create(path, new Document("foo"));
        transaction.commit();

        ConfigurationController<ContractScope> controller = ConfigurationController.construct(scope);
        Assert.assertTrue(controller.exists(path));
    }

    @Test(groups = "unit")
    public void testTenantTransaction() throws Exception {
        TenantScope scope = new TenantScope(CamilleTestEnvironment.getContractId(),
                CamilleTestEnvironment.getTenantId());
        Path path = new Path("/foo");

        ConfigurationTransaction<TenantScope> transaction = ConfigurationTransaction.construct(scope);
        transaction.create(path, new Document("foo"));
        transaction.commit();

        ConfigurationController<TenantScope> controller = ConfigurationController.construct(scope);
        Assert.assertTrue(controller.exists(path));
    }
}
