package com.latticeengines.admin.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;

public class TenantResourceTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private TestLatticeComponent testLatticeComponent;

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void getTenantsWithContractId() {
        String url = getRestHostPort() + "/admin/tenants?contractId=" + TestContractId;
        assertSingleTenant(restTemplate.getForObject(url, List.class, new HashMap<>()));
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void getTenantsWithNoContractId() {
        String url = getRestHostPort() + "/admin/tenants";
        assertSingleTenant(restTemplate.getForObject(url, List.class, new HashMap<>()));
    }

    private void assertSingleTenant(List<Map<String, Object>> tenantObjs) {
        Assert.assertTrue(tenantObjs.size() >= 1);
        boolean existTENANT1 = false;
        ObjectMapper mapper = new ObjectMapper();
        try {
            for (Map<String, Object> obj : tenantObjs) {
                TenantDocument tenant = mapper.treeToValue(mapper.valueToTree(obj), TenantDocument.class);
                if (tenant.getSpace().getTenantId().equals(TestTenantId)) {
                    existTENANT1 = true;
                    break;
                }
            }
            Assert.assertTrue(existTENANT1);
        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test(groups = "functional", timeOut = 10000)
    public void getServiceState() throws InterruptedException {
        String url = getRestHostPort() + String.format("/admin/tenants/%s/services/%s/state?contractId=%s",
                TestTenantId, testLatticeComponent.getName(), TestContractId);
        bootstrap(TestContractId, TestTenantId, testLatticeComponent.getName());

        BootstrapState state;
        int numTries = 0;
        do {
            state = restTemplate.getForObject(url, BootstrapState.class, new HashMap<>());
            Thread.sleep(1000L);
            numTries++;
        } while (state.state != BootstrapState.State.OK && numTries < 10);
        Assert.assertEquals(state.state, BootstrapState.State.OK);
    }

    @Test(groups = "functional")
    public void getServiceConfig() {
        String url = getRestHostPort() + String.format("/admin/tenants/%s/services/%s/?contractId=%s",
                TestTenantId, testLatticeComponent.getName(), TestContractId);
        SerializableDocumentDirectory dir =
                restTemplate.getForObject(url, SerializableDocumentDirectory.class, new HashMap<>());
        Assert.assertNotNull(dir);
    }

    @Test(groups = "functional")
    public void getFeatureFlags() {
        defineFeatureFlagByRestCall(FLAG_ID, FLAG_DEFINITION);

        String url = getRestHostPort() + String.format("/admin/tenants/%s/featureflags", TestTenantId);
        FeatureFlagValueMap flags = restTemplate.getForObject(url, FeatureFlagValueMap.class);
        Assert.assertFalse(flags.containsKey(FLAG_ID), "Flag should not have been set.");

        FeatureFlagValueMap flagsToSet = new FeatureFlagValueMap();
        flagsToSet.put(FLAG_ID, true);
        flags = restTemplate.postForObject(url, flagsToSet, FeatureFlagValueMap.class);
        Assert.assertTrue(flags.containsKey(FLAG_ID), "Flag should have been set.");
        Assert.assertTrue(flags.get(FLAG_ID));

        restTemplate.delete(url + "/" + FLAG_ID);
        flags = restTemplate.getForObject(url, FeatureFlagValueMap.class);
        Assert.assertFalse(flags.containsKey(FLAG_ID), "Flag should have been removed.");

        undefineFeatureFlagByRestCall(FLAG_ID);
    }

    @Test(groups = "functional")
    public void setFeatureFlagsWithUndefinedFlags() {
        defineFeatureFlagByRestCall(FLAG_ID, FLAG_DEFINITION);

        FeatureFlagValueMap flagsToSet = new FeatureFlagValueMap();
        flagsToSet.put(FLAG_ID, true);
        flagsToSet.put("Nope", true);
        boolean hasException = false;
        try {
            String url = getRestHostPort() + String.format("/admin/tenants/%s/featureflags", TestTenantId);
            restTemplate.postForObject(url, flagsToSet, FeatureFlagValueMap.class);
        } catch (Exception e) {
            hasException = true;
        }
        Assert.assertTrue(hasException, "Should raise exception due to undefined flag.");

        undefineFeatureFlagByRestCall(FLAG_ID);
    }
}
