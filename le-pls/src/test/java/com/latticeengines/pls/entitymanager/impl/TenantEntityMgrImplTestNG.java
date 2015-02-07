package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class TenantEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    @BeforeClass(groups = "functional")
    public void setup() {
        List<Tenant> tenants = tenantEntityMgr.findAll();

        for (Tenant tenant : tenants) {
            tenantEntityMgr.delete(tenant);
        }
        
        Tenant tenant = new Tenant();
        tenant.setId("TENANT1");
        tenant.setName("TENANT1");
        tenantEntityMgr.create(tenant);
    }
    
    @Test(groups = "functional")
    public void findByTenantId() {
        Tenant t = tenantEntityMgr.findByTenantId("TENANT1");
        assertEquals(t.getName(), "TENANT1");
    }

}
