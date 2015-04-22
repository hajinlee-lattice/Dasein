package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.UserService;


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
            globalTenantManagementService.discardTenant(tenant);
        } catch(LedpException e) {
            //ignore
        }
    }

    @AfterClass(groups = {"functional"})
    public void tearDown() {
        try {
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
