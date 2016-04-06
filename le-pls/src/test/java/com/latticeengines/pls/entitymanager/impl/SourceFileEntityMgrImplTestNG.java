package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;

public class SourceFileEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    @Test(groups = "functional")
    public void testGetApplicationId() {
        Tenant tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);
        setupSecurityContext(tenant1);

        String name = "SomeFile";
        String applicationId = "applicationId_00010";
        String path = "path";
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName(name);
        sourceFile.setPath(path);
        sourceFile.setApplicationId(applicationId);
        sourceFileEntityMgr.create(sourceFile);

        SourceFile sourceFile2 = new SourceFile();
        sourceFile2.setName(name + "2");
        sourceFile2.setPath(path + "2");
        sourceFile2.setApplicationId(applicationId + "2");
        sourceFileEntityMgr.create(sourceFile2);

        SourceFile sourceFile3 = sourceFileEntityMgr.findByApplicationId(applicationId);
        assertEquals(sourceFile.getName(), sourceFile3.getName());
        assertEquals(sourceFile.getPath(), sourceFile3.getPath());
        assertEquals(sourceFile.getPid(), sourceFile3.getPid());
    }
}
