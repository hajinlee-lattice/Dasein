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
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;

public class ContractLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final ContractInfo contractInfo = CamilleTestEnvironment.getContractInfo();

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
        String contractId = "testContract";
        ContractLifecycleManager.create(contractId, contractInfo);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId)));
        ContractLifecycleManager.create(contractId, contractInfo);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        String contractId = "testContract";
        ContractLifecycleManager.delete(contractId);
        ContractLifecycleManager.create(contractId, contractInfo);
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
        ContractLifecycleManager.create(contractId, contractInfo);
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
            ContractLifecycleManager.create(contractId, contractInfo);
        }

        List<AbstractMap.SimpleEntry<String, ContractInfo>> all = ContractLifecycleManager.getAll();
        List<String> allContracts = new ArrayList<String>();
        for (AbstractMap.SimpleEntry<String, ContractInfo> pair : all) {
            allContracts.add(pair.getKey());
        }
        Assert.assertTrue(allContracts.containsAll(in));
    }
}
