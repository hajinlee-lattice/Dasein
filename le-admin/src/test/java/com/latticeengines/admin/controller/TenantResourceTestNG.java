package com.latticeengines.admin.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class TenantResourceTestNG extends AdminFunctionalTestNGBase {
    
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
        assertEquals(tenants.size(), 1);
        Map<String, Object> map = tenants.get(0);
        assertEquals((String) map.get("key"), "TENANT1");
    }
    
    @Test(groups = "functional")
    public void getServiceState() {
        String url = getRestHostPort() + "/admin/tenants/TENANT1/services/TestComponent/state?contractId=CONTRACT1";
        BootstrapState state = restTemplate.getForObject(url, BootstrapState.class, new HashMap<>());
        assertNotNull(state);
        assertEquals(state.state, BootstrapState.State.OK);
    }
    
    @Test(groups = "functional")
    public void getServiceConfig() {
        String url = getRestHostPort() + "/admin/tenants/TENANT1/services/TestComponent?contractId=CONTRACT1";
        SerializableDocumentDirectory dir = restTemplate.getForObject(url, SerializableDocumentDirectory.class, new HashMap<>());
        assertNotNull(dir);
    }
    
}
