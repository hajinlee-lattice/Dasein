package com.latticeengines.security.exposed.globalauth.impl;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class GlobalTenantManagementServiceImplTestNG extends SecurityFunctionalTestNGBase {
    
    @Autowired
    private GlobalTenantManagementService globalTenantManagementService;

    @Test(groups = "functional")
    public void registerThenDiscardTenant() {
        Tenant newTenant = new Tenant();
        newTenant.setId("TEST_TENANT_NEW");
        newTenant.setName("TEST_TENANT_NEW");
        
        assertTrue(globalTenantManagementService.registerTenant(newTenant));
        assertTrue(globalTenantManagementService.discardTenant(newTenant));
    }
}

