package com.latticeengines.camille.lifecycle;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.CamilleTestEnvironment;
import com.latticeengines.camille.paths.PathBuilder;

public class SpaceLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final String contractId = "testContractId";
    private static final String tenantId = "testTenantId";

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(contractId);
        TenantLifecycleManager.create(contractId, tenantId);
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testCreateDefault() throws Exception {
        SpaceLifecycleManager.createDefault(contractId, tenantId);
        Assert.assertEquals(
                1,
                CamilleEnvironment
                        .getCamille()
                        .getChildren(
                                PathBuilder.buildCustomerSpacesPath(CamilleEnvironment.getPodId(), contractId, tenantId))
                        .size());
    }

    @Test(groups = "unit")
    public void testCreate() throws Exception {
        String spaceId = "testSpace";
        SpaceLifecycleManager.create(contractId, tenantId, spaceId);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId)));
        SpaceLifecycleManager.create(contractId, tenantId, spaceId);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        String spaceId = "testSpace";
        SpaceLifecycleManager.delete(contractId, tenantId, spaceId);
        SpaceLifecycleManager.create(contractId, tenantId, spaceId);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId)));
        SpaceLifecycleManager.delete(contractId, tenantId, spaceId);
        Assert.assertFalse(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId)));
    }

    @Test(groups = "unit")
    public void testExists() throws Exception {
        String spaceId = "testSpace";
        Assert.assertFalse(SpaceLifecycleManager.exists(contractId, tenantId, spaceId));
        SpaceLifecycleManager.create(contractId, tenantId, spaceId);
        Assert.assertTrue(SpaceLifecycleManager.exists(contractId, tenantId, spaceId));
        SpaceLifecycleManager.delete(contractId, tenantId, spaceId);
        Assert.assertFalse(SpaceLifecycleManager.exists(contractId, tenantId, spaceId));
    }

    @Test(groups = "unit")
    public void testGetAll() throws Exception {
        Set<String> in = new HashSet<String>();
        for (int i = 0; i < 10; ++i) {
            String spaceId = Integer.toString(i);
            in.add(spaceId);
            SpaceLifecycleManager.create(contractId, tenantId, spaceId);
        }
        Assert.assertTrue(SpaceLifecycleManager.getAll(contractId, tenantId).containsAll(in));
    }
}
