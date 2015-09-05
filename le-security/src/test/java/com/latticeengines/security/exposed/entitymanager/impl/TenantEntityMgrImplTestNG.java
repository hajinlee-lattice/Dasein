package com.latticeengines.security.exposed.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class TenantEntityMgrImplTestNG extends SecurityFunctionalTestNGBase {

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantService tenantService;

    @BeforeClass(groups = "functional")
    public void setup() {
        Tenant tenant = new Tenant();
        tenant.setId("TENANT1");
        tenant.setName("TENANT1");

        try {
            tenantService.discardTenant(tenant);
        } catch (Exception e) {
            // ignore
        }
        tenantEntityMgr.create(tenant);
    }

    @Test(groups = "functional")
    public void findByTenantId() {
        Tenant t = tenantEntityMgr.findByTenantId("TENANT1");
        assertEquals(t.getName(), "TENANT1");
    }

}
