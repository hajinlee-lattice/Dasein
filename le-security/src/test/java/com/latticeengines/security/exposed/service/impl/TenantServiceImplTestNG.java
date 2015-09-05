package com.latticeengines.security.exposed.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;


public class TenantServiceImplTestNG extends SecurityFunctionalTestNGBase {

    @Autowired
    TenantService tenantService;

    @Autowired
    GlobalTenantManagementService globalTenantManagementService;

    @BeforeClass(groups = "functional")
    public void setup() {
        Tenant tenant = createNewTenant();
        try {
            tenantService.discardTenant(tenant);
            globalTenantManagementService.discardTenant(tenant);
        } catch(LedpException e) {
            //ignore
        }
    }

    @AfterClass(groups = {"functional"})
    public void tearDown() {
        Tenant tenant = createNewTenant();
        try {
            tenantService.discardTenant(tenant);
            globalTenantManagementService.discardTenant(tenant);
        } catch(LedpException e) {
            //ignore
        }
    }

    @Test(groups = "functional")
    public void testRegisterAndDiscardTenant() {
        Tenant tenant = createNewTenant();

        tenantService.registerTenant(tenant);
        tenantService.discardTenant(tenant);
    }

    @Test(groups = "functional")
    public void testHasTenantWithId() {
        Tenant tenant = createNewTenant();

        Assert.assertFalse(tenantService.hasTenantId(tenant.getId()));
        tenantService.registerTenant(tenant);
        Assert.assertTrue(tenantService.hasTenantId(tenant.getId()));
        tenantService.discardTenant(tenant);
        Assert.assertFalse(tenantService.hasTenantId(tenant.getId()));
    }

    @Test(groups = "functional", dependsOnMethods = {"testRegisterAndDiscardTenant", "testHasTenantWithId"})
    public void testUpdateInsteadOfCreate() {
        Tenant tenant = createNewTenant();

        tenantService.registerTenant(tenant);
        Tenant newTenant = tenantService.findByTenantId(tenant.getId());
        Assert.assertEquals(newTenant.getName(), tenant.getName());

        tenant.setName("New Name");
        tenantService.updateTenant(tenant);
        newTenant = tenantService.findByTenantId(tenant.getId());
        Assert.assertEquals(newTenant.getName(), "New Name");
    }

    private Tenant createNewTenant() {
        Tenant tenant = new Tenant();
        tenant.setName("TenantService Test Tenant");
        tenant.setId("TenantService_Test_Tenant");
        return tenant;
    }

}
