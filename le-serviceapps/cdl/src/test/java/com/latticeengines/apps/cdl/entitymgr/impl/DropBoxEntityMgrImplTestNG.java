package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DropBoxEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.DBConnectionContext;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.security.Tenant;

public class DropBoxEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DropBoxEntityMgr entityMgr;

    @Value("${aws.customer.s3.region}")
    private String region;

    private Tenant tenant1;
    private Tenant tenant2;

    @BeforeClass(groups = "functional")
    public void setup() {
        testBed.bootstrap(2);
        tenant1 = testBed.getTestTenants().get(0);
        tenant2 = testBed.getTestTenants().get(1);
    }

    @Test(groups = "functional")
    public void testCrud() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant1.getId()));
        DropBox dropbox1 = entityMgr.createDropBox(region);
        Assert.assertNotNull(dropbox1.getPid());
        Assert.assertNotNull(dropbox1.getRegion());
        Assert.assertEquals(dropbox1.getTenant().getId(), tenant1.getId());

        Assert.assertTrue(StringUtils.isBlank(dropbox1.getEncryptedSecretKey()));
        dropbox1.setEncryptedSecretKey("TestKey");
        entityMgr.update(dropbox1);
        dropbox1 = entityMgr.getDropBox();
        Assert.assertTrue(StringUtils.isNotBlank(dropbox1.getEncryptedSecretKey()));

        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant2.getId()));
        DropBox dropbox2 = entityMgr.createDropBox(region);
        Assert.assertNotNull(dropbox2.getPid());
        Assert.assertNotNull(dropbox2.getRegion());
        Assert.assertEquals(dropbox2.getTenant().getId(), tenant2.getId());

        DropBox dropbox = entityMgr.getDropBox();
        Assert.assertEquals(dropbox.getPid(), dropbox2.getPid());
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant1.getId()));
        dropbox = entityMgr.getDropBox();
        Assert.assertEquals(dropbox.getPid(), dropbox1.getPid());

        DBConnectionContext.setReaderConnection(true);
        dropbox = entityMgr.getDropBox();
        Assert.assertEquals(dropbox.getPid(), dropbox1.getPid());
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant2.getId()));
        dropbox = entityMgr.getDropBox();
        Assert.assertEquals(dropbox.getPid(), dropbox2.getPid());
    }

}
