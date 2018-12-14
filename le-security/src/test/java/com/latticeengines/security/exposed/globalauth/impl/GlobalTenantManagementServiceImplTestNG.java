package com.latticeengines.security.exposed.globalauth.impl;

import static org.testng.Assert.assertTrue;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class GlobalTenantManagementServiceImplTestNG extends SecurityFunctionalTestNGBase {

    @Inject
    private GlobalTenantManagementService globalTenantManagementService;

    @Test(groups = "functional")
    public void registerThenDiscardTenant() {
        Tenant newTenant = new Tenant();
        newTenant.setId(NamingUtils.timestamp("TestTenant"));
        newTenant.setName("Test Tenant");

        assertTrue(globalTenantManagementService.registerTenant(newTenant));
        assertTrue(globalTenantManagementService.discardTenant(newTenant));
    }
}

