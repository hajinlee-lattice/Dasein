package com.latticeengines.pls.globalauth.authentication.impl;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalTenantManagementService;

public class GlobalTenantManagementServiceImplTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private GlobalTenantManagementService globalTenantManagementService;

    @Test(groups = "functional")
    public void registerThenDiscardTenant() {
        Tenant newTenant = new Tenant();
        newTenant.setId("NEW_TENANT");
        newTenant.setName("NEW_TENANT");
        
        assertTrue(globalTenantManagementService.registerTenant(newTenant));
        assertTrue(globalTenantManagementService.discardTenant(newTenant));
    }
}

