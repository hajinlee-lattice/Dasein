package com.latticeengines.domain.exposed.security;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class TenantUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        Tenant tenant = new Tenant();
        tenant.setName("Tenant1");
        String deserializedStr = tenant.toString();
        Tenant deserializedTenant = JsonUtils.deserialize(deserializedStr, Tenant.class);
        
        assertEquals(tenant.getName(), deserializedTenant.getName());
    }
}
