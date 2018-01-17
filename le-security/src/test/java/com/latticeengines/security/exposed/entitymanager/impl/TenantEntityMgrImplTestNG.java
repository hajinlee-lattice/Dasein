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

    private Long tenantPid;

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
        tenantPid = t.getPid();
        assertEquals(t.getName(), "TENANT1");
    }

    @Test(groups = "functional", dependsOnMethods = "findByTenantId")
    public void findByTenantPid() {
        Tenant t = tenantEntityMgr.findByTenantPid(tenantPid);
        assertEquals(t.getId(), "TENANT1");
        assertEquals(t.getName(), "TENANT1");
    }

    @Test(groups = "functional")
    public void findByTenantName() {
        Tenant t = tenantEntityMgr.findByTenantName("TENANT1");
        assertEquals(t.getId(), "TENANT1");
    }
}
