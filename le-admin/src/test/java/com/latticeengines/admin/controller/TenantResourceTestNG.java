package com.latticeengines.admin.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class TenantResourceTestNG extends AdminFunctionalTestNGBase {

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        loginAD();
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void getTenantsWithContractId() {
        String url = getRestHostPort() + "/admin/tenants?contractId=CONTRACT1";
        assertSingleTenant(restTemplate.getForObject(url, List.class, new HashMap<>()));
    }
    
    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void getTenantsWithNoContractId() {
        String url = getRestHostPort() + "/admin/tenants";
        assertSingleTenant(restTemplate.getForObject(url, List.class, new HashMap<>()));
    }
    
    private void assertSingleTenant(List<Map<String, Object>> tenants) {
        // Deserialization of List<AbstractMap.Entry> is strange from the testing perspective
        // In practice, only JS will be accessing this REST endpoint, so will let JS figure out how to best
        // handle this deserialization
        Assert.assertTrue(tenants.size() >= 1);
        boolean existTENANT1 = false;
        for (Map<String, Object> map : tenants) {
            if (map.get("key").equals("TENANT1")) {
                existTENANT1 = true;
                break;
            }
        }
        Assert.assertTrue(existTENANT1);
    }
    
    @Test(groups = "functional")
    public void getServiceState() throws InterruptedException {
        String url = getRestHostPort() + "/admin/tenants/TENANT1/services/TestComponent/state?contractId=CONTRACT1";

        BootstrapState state;
        int numTries = 0;
        do {
            state = restTemplate.getForObject(url, BootstrapState.class, new HashMap<>());
            Thread.sleep(1000L);
            numTries++;
        } while (state.state != BootstrapState.State.OK && numTries < 5);
        Assert.assertEquals(state.state, BootstrapState.State.OK);
    }
    
    @Test(groups = "functional")
    public void getServiceConfig() {
        String url = getRestHostPort() + "/admin/tenants/TENANT1/services/TestComponent?contractId=CONTRACT1";
        SerializableDocumentDirectory dir = restTemplate.getForObject(url, SerializableDocumentDirectory.class, new HashMap<>());
        Assert.assertNotNull(dir);
    }
    
}
