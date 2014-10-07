package com.latticeengines.camille.lifecycle;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.Camille;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.CamilleTestEnvironment;
import com.latticeengines.camille.paths.PathBuilder;

public class TenantLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final String contractId = "testContractId";

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(contractId);
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testCreateNullDefaultSpaceAndSetDefaultSpace() throws Exception {
        String tenantId = "testTenant";
        Camille camille = CamilleEnvironment.getCamille();
        TenantLifecycleManager.create(contractId, tenantId);
        Assert.assertTrue(camille.exists(PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId,
                tenantId)));
        TenantLifecycleManager.create(contractId, tenantId);
        String defaultSpaceId1 = TenantLifecycleManager.getDefaultSpaceId(contractId, tenantId);
        Assert.assertNotNull(defaultSpaceId1);
        Assert.assertTrue(camille.exists(PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId,
                tenantId, defaultSpaceId1)));
        String defaultSpaceId2 = "testDefaultSpaceId";
        try {
            TenantLifecycleManager.setDefaultSpaceId(contractId, tenantId, defaultSpaceId2);
            Assert.fail(String.format("Space with spaceId=%s does not exist.  setDefaultSpaceId should have failed.",
                    defaultSpaceId2));
        } catch (RuntimeException e) {
        }
        SpaceLifecycleManager.create(contractId, tenantId, defaultSpaceId2);
        TenantLifecycleManager.setDefaultSpaceId(contractId, tenantId, defaultSpaceId2);
        Assert.assertEquals(TenantLifecycleManager.getDefaultSpaceId(contractId, tenantId), defaultSpaceId2);
    }

    @Test(groups = "unit")
    public void testCreateNotNullDefaultSpaceAndSetDefaultSpace() throws Exception {
        String tenantId = "testTenant";
        String defaultSpaceId1 = "testDefaultSpaceId1";

        TenantLifecycleManager.create(contractId, tenantId, defaultSpaceId1);

        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId)));

        // should fail gracefully without modifying .default-space
        TenantLifecycleManager.create(contractId, tenantId);

        Assert.assertEquals(defaultSpaceId1, TenantLifecycleManager.getDefaultSpaceId(contractId, tenantId));
        String defaultSpaceId2 = "testDefaultSpaceId2";
        try {
            TenantLifecycleManager.setDefaultSpaceId(contractId, tenantId, defaultSpaceId2);
            Assert.fail(String.format("Space with spaceId=%s does not exist.  setDefaultSpaceId should have failed.",
                    defaultSpaceId2));
        } catch (RuntimeException e) {
        }
        SpaceLifecycleManager.create(contractId, tenantId, defaultSpaceId2);
        TenantLifecycleManager.setDefaultSpaceId(contractId, tenantId, defaultSpaceId2);
        Assert.assertEquals(TenantLifecycleManager.getDefaultSpaceId(contractId, tenantId), defaultSpaceId2);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        String tenantId = "testTenant";
        TenantLifecycleManager.delete(contractId, tenantId);
        TenantLifecycleManager.create(contractId, tenantId);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId)));
        TenantLifecycleManager.delete(contractId, tenantId);
        Assert.assertFalse(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId)));
    }

    @Test(groups = "unit")
    public void testExists() throws Exception {
        String tenantId = "testTenant";
        Assert.assertFalse(TenantLifecycleManager.exists(contractId, tenantId));
        TenantLifecycleManager.create(contractId, tenantId);
        Assert.assertTrue(TenantLifecycleManager.exists(contractId, tenantId));
        TenantLifecycleManager.delete(contractId, tenantId);
        Assert.assertFalse(TenantLifecycleManager.exists(contractId, tenantId));
    }

    @Test(groups = "unit")
    public void testGetAll() throws Exception {
        Set<String> in = new HashSet<String>();
        for (int i = 0; i < 10; ++i) {
            String tenantId = Integer.toString(i);
            in.add(tenantId);
            TenantLifecycleManager.create(contractId, tenantId);
        }
        Assert.assertTrue(in.containsAll(TenantLifecycleManager.getAll(contractId)));
    }
}
