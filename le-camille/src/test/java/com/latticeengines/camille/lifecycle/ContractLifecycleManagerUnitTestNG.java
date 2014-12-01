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
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.camille.util.CamilleTestEnvironment;

public class ContractLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testCreate() throws Exception {
        String contractId = "testContract";
        ContractLifecycleManager.create(contractId);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId)));
        ContractLifecycleManager.create(contractId);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        String contractId = "testContract";
        ContractLifecycleManager.delete(contractId);
        ContractLifecycleManager.create(contractId);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId)));
        ContractLifecycleManager.delete(contractId);
        Assert.assertFalse(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId)));
    }

    @Test(groups = "unit")
    public void testExists() throws Exception {
        String contractId = "testContract";
        Assert.assertFalse(ContractLifecycleManager.exists(contractId));
        ContractLifecycleManager.create(contractId);
        Assert.assertTrue(ContractLifecycleManager.exists(contractId));
        ContractLifecycleManager.delete(contractId);
        Assert.assertFalse(ContractLifecycleManager.exists(contractId));
    }

    @Test(groups = "unit")
    public void testGetAll() throws Exception {
        Set<String> in = new HashSet<String>();
        for (int i = 0; i < 10; ++i) {
            String contractId = Integer.toString(i);
            in.add(contractId);
            ContractLifecycleManager.create(contractId);
        }
        Assert.assertTrue(in.containsAll(ContractLifecycleManager.getAll()));
    }
}
