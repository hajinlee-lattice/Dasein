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
import com.latticeengines.domain.exposed.camille.Contract;
import com.latticeengines.domain.exposed.camille.Pod;

public class ContractLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        PodLifecycleManager.create(new Pod(CamilleEnvironment.getPodId()));
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testCreate() throws Exception {
        Contract contract = new Contract("testContract");
        ContractLifecycleManager.create(contract);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contract.getContractId())));
        ContractLifecycleManager.create(contract);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        ContractLifecycleManager.delete("testContract");
        Contract contract = new Contract("testContract");
        ContractLifecycleManager.create(contract);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contract.getContractId())));
        ContractLifecycleManager.delete("testContract");
        Assert.assertFalse(CamilleEnvironment.getCamille().exists(
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contract.getContractId())));
    }

    @Test(groups = "unit")
    public void testExists() throws Exception {
        Assert.assertFalse(ContractLifecycleManager.exists("testContract"));
        Contract contract = new Contract("testContract");
        ContractLifecycleManager.create(contract);
        Assert.assertTrue(ContractLifecycleManager.exists("testContract"));
        ContractLifecycleManager.delete("testContract");
        Assert.assertFalse(ContractLifecycleManager.exists("testContract"));
    }

    @Test(groups = "unit")
    public void testGet() throws Exception {
        Contract in = new Contract("testContract");
        ContractLifecycleManager.create(in);
        Contract out = ContractLifecycleManager.get(in.getContractId());
        Assert.assertEquals(in, out);
    }

    @Test(groups = "unit")
    public void testGetAll() throws Exception {
        Set<Contract> in = new HashSet<Contract>();
        for (int i = 0; i < 10; ++i) {
            Contract c = new Contract(Integer.toString(i));
            in.add(c);
            ContractLifecycleManager.create(c);
        }
        Assert.assertTrue(in.containsAll(ContractLifecycleManager.getAll()));
    }
}
