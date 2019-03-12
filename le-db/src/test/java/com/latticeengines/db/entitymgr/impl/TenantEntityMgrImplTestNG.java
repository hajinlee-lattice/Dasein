package com.latticeengines.db.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.testframework.DbFunctionalTestNGBase;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;

public class TenantEntityMgrImplTestNG extends DbFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TenantEntityMgrImplTestNG.class);

    private static final String TENANT_ID = "TENANT1";

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    private Long tenantPid;

    @BeforeClass(groups = "functional")
    public void setup() {
        Tenant tenant = new Tenant();
        tenant.setId(TENANT_ID);
        tenant.setName(TENANT_ID);
        tenant.setTenantType(TenantType.QA);

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

    @Test(groups = "functional")
    public void findAllByStatus() {
        List<Tenant> tenantList = tenantEntityMgr.findAllByStatus(TenantStatus.ACTIVE);
        Tenant t = tenantEntityMgr.findByTenantName(TENANT_ID);
        assertTrue(tenantList.contains(t));
    }

    @Test(groups = "functional")
    public void findAllByTenantType() {
        List<Tenant> tenantList = tenantEntityMgr.findAllByType(TenantType.QA);
        Assert.assertNotNull(tenantList);
        Tenant t = tenantEntityMgr.findByTenantName(TENANT_ID);
        assertTrue(tenantList.contains(t));
        for (Tenant tenant : tenantList) {
            Assert.assertEquals(tenant.getTenantType(), TenantType.QA);
        }
    }

}
