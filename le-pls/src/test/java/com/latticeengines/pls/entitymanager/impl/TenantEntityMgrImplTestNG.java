package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantService;

public class TenantEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

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
