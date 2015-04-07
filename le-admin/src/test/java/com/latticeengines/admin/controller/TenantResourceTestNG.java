package com.latticeengines.admin.controller;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;

public class TenantResourceTestNG extends AdminFunctionalTestNGBase {
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        if (TenantLifecycleManager.exists("CONTRACT1", "TENANT1")) {
            TenantLifecycleManager.delete("CONTRACT1", "TENANT1");
        }
        super.createTenant();
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
        assertEquals(tenants.size(), 1);
        Map<String, Object> map = tenants.get(0);
        
        assertEquals((String) map.get("key"), "TENANT1");
    }
}
