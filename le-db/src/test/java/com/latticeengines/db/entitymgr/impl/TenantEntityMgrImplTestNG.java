package com.latticeengines.db.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.testframework.DbFunctionalTestNGBase;
import com.latticeengines.domain.exposed.security.Tenant;

public class TenantEntityMgrImplTestNG extends DbFunctionalTestNGBase {

    private static final String TENANT_ID = "TENANT1";

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    private Long tenantPid;

    @BeforeClass(groups = "functional")
    public void setup() {
        Tenant tenant = new Tenant();
        tenant.setId(TENANT_ID);
        tenant.setName(TENANT_ID);

        if (tenantEntityMgr.findByTenantId(TENANT_ID) != null) {
            tenantEntityMgr.delete(tenant);
        }

        tenantEntityMgr.create(tenant);
    }

    @Test(groups = "functional")
    public void findByTenantId() {
        Tenant t = tenantEntityMgr.findByTenantId(TENANT_ID);
        tenantPid = t.getPid();
        assertEquals(t.getName(), TENANT_ID);
    }

    @Test(groups = "functional", dependsOnMethods = "findByTenantId")
    public void findByTenantPid() {
        Tenant t = tenantEntityMgr.findByTenantPid(tenantPid);
        assertEquals(t.getId(), TENANT_ID);
        assertEquals(t.getName(), TENANT_ID);
    }

    @Test(groups = "functional")
    public void findByTenantName() {
        Tenant t = tenantEntityMgr.findByTenantName(TENANT_ID);
        assertEquals(t.getId(), TENANT_ID);
    }
}
