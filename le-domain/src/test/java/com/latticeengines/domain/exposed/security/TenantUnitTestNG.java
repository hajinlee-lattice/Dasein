package com.latticeengines.domain.exposed.security;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class TenantUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        Tenant tenant = new Tenant();
        tenant.setName("Tenant1");
        tenant.setId("Tenant1");
        String serializedStr = tenant.toString();
        System.out.println(serializedStr);
        Tenant deserializedTenant = JsonUtils.deserialize(serializedStr, Tenant.class);
        
        assertEquals(tenant.getName(), deserializedTenant.getName());
    }
}
