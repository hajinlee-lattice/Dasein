package com.latticeengines.camille.lifecycle;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public class TenantLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final String contractId = CamilleTestEnvironment.getContractId();
    private static final CustomerSpaceInfo customerSpaceInfo = CamilleTestEnvironment.getCustomerSpaceInfo();
    private static final TenantInfo tenantInfo = CamilleTestEnvironment.getTenantInfo();

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testCreateNotNullDefaultSpaceAndSetDefaultSpace() throws Exception {
        String tenantId = "testTenant";
        String defaultSpaceId1 = "testDefaultSpaceId1";

        TenantLifecycleManager.create(contractId, tenantId, tenantInfo, defaultSpaceId1, customerSpaceInfo);

        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId)));

        Assert.assertEquals(defaultSpaceId1, TenantLifecycleManager.getDefaultSpaceId(contractId, tenantId));
        String defaultSpaceId2 = "testDefaultSpaceId2";
        try {
            TenantLifecycleManager.setDefaultSpaceId(contractId, tenantId, defaultSpaceId2);
            Assert.fail(String.format("Space with spaceId=%s does not exist.  setDefaultSpaceId should have failed.",
                    defaultSpaceId2));
        } catch (RuntimeException e) {
        }
        SpaceLifecycleManager.create(contractId, tenantId, defaultSpaceId2, customerSpaceInfo);
        TenantLifecycleManager.setDefaultSpaceId(contractId, tenantId, defaultSpaceId2);
        Assert.assertEquals(TenantLifecycleManager.getDefaultSpaceId(contractId, tenantId), defaultSpaceId2);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        String tenantId = "testTenant";
        String spaceId = "spaceId";
        TenantLifecycleManager.delete(contractId, tenantId);
        TenantLifecycleManager.create(contractId, tenantId, tenantInfo, spaceId, customerSpaceInfo);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId)));
        TenantLifecycleManager.delete(contractId, tenantId);
        Assert.assertFalse(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId)));
    }

    @Test(groups = "unit")
    public void testExists() throws Exception {
        String tenantId = "testTenant";
        String spaceId = "spaceId";
        Assert.assertFalse(TenantLifecycleManager.exists(contractId, tenantId));
        TenantLifecycleManager.create(contractId, tenantId, tenantInfo, spaceId, customerSpaceInfo);
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
            TenantLifecycleManager.create(contractId, tenantId, tenantInfo, "defaultSpaceId", customerSpaceInfo);
        }

        List<AbstractMap.SimpleEntry<String, TenantInfo>> all = TenantLifecycleManager.getAll(contractId);
        List<String> allTenants = new ArrayList<String>();
        for (AbstractMap.SimpleEntry<String, TenantInfo> pair : all) {
            allTenants.add(pair.getKey());
        }
        Assert.assertTrue(allTenants.containsAll(in));
    }
}
