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
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;

public class SpaceLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final String contractId = CamilleTestEnvironment.getContractId();
    private static final String tenantId = CamilleTestEnvironment.getTenantId();
    private static final CustomerSpaceInfo customerSpaceInfo = CamilleTestEnvironment.getCustomerSpaceInfo();

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testCreate() throws Exception {
        String spaceId = "testSpace";
        SpaceLifecycleManager.create(contractId, tenantId, spaceId, customerSpaceInfo);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId)));
        SpaceLifecycleManager.create(contractId, tenantId, spaceId, customerSpaceInfo);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        String spaceId = "testSpace";
        SpaceLifecycleManager.delete(contractId, tenantId, spaceId);
        SpaceLifecycleManager.create(contractId, tenantId, spaceId, customerSpaceInfo);
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
        SpaceLifecycleManager.create(contractId, tenantId, spaceId, customerSpaceInfo);
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
            SpaceLifecycleManager.create(contractId, tenantId, spaceId, customerSpaceInfo);
        }

        List<AbstractMap.SimpleEntry<String, CustomerSpaceInfo>> all = SpaceLifecycleManager.getAll(contractId, tenantId);
        List<String> allSpaces = new ArrayList<String>();
        for (AbstractMap.SimpleEntry<String, CustomerSpaceInfo> pair : all) {
            allSpaces.add(pair.getKey());
        }
        Assert.assertTrue(allSpaces.containsAll(in));
    }

    @Test(groups = "unit")
    public void testGetAllRecursive() throws Exception {
        Set<String> in = new HashSet<String>();

        String secondTenantId = tenantId + "2";
        TenantLifecycleManager.create(contractId, secondTenantId, CamilleTestEnvironment.getTenantInfo(),
                CamilleTestEnvironment.getSpaceId(), CamilleTestEnvironment.getCustomerSpaceInfo());

        for (int i = 0; i < 10; ++i) {
            String spaceId = Integer.toString(i);
            in.add(spaceId);
            SpaceLifecycleManager.create(contractId, tenantId, spaceId, customerSpaceInfo);
        }

        for (int i = 10; i < 20; ++i) {
            String spaceId = Integer.toString(i);
            in.add(spaceId);
            SpaceLifecycleManager.create(contractId, secondTenantId, spaceId, customerSpaceInfo);
        }

        List<AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo>> all = SpaceLifecycleManager.getAll();
        List<String> allSpaces = new ArrayList<String>();
        for (AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo> pair : all) {
            allSpaces.add(pair.getKey().getSpaceId());
        }
        Assert.assertTrue(allSpaces.containsAll(in));
    }

}
