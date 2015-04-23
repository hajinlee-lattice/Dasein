package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;


public class TenantServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    TenantService tenantService;

    @Autowired
    GlobalTenantManagementService globalTenantManagementService;

    private Tenant tenant;

    @BeforeClass(groups = "functional")
    public void setup() {
        tenant = new Tenant();
        tenant.setName("TenantService Test Tenant");
        tenant.setId("TenantService_Test_Tenant");
        try {
            tenantService.discardTenant(tenant);
            globalTenantManagementService.discardTenant(tenant);
        } catch(LedpException e) {
            //ignore
        }
    }

    @AfterClass(groups = {"functional"})
    public void tearDown() {
        try {
            tenantService.discardTenant(tenant);
            globalTenantManagementService.discardTenant(tenant);
        } catch(LedpException e) {
            //ignore
        }
    }

    @Test(groups = "functional")
    public void testRegisterAndDiscardTenant() {
        tenantService.discardTenant(tenant);
        tenantService.registerTenant(tenant);
        tenantService.discardTenant(tenant);
    }

}
