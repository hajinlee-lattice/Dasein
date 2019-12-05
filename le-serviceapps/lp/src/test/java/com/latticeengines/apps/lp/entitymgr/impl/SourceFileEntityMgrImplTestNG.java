package com.latticeengines.apps.lp.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.entitymgr.SourceFileEntityMgr;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

public class SourceFileEntityMgrImplTestNG extends LPFunctionalTestNGBase {

    @Inject
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Inject
    private TenantService tenantService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);
        MultiTenantContext.setTenant(tenant1);
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
        String name = "SomeFileForApplicationId";
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

    @Test(groups = "functional")
    public void testGetWorkflowPid() {
        String name = "SomeFileForWorkflowPid";
        Long workflowPid1 = Long.valueOf(111);
        Long workflowPid2 = Long.valueOf(222);
        String path = "path";
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName(name);
        sourceFile.setPath(path);
        sourceFile.setWorkflowPid(workflowPid1);
        sourceFileEntityMgr.create(sourceFile);

        SourceFile sourceFile2 = new SourceFile();
        sourceFile2.setName(name + "2");
        sourceFile2.setPath(path + "2");
        sourceFile.setWorkflowPid(workflowPid2);
        sourceFileEntityMgr.create(sourceFile2);

        SourceFile sourceFile3 = sourceFileEntityMgr.findByWorkflowPid(workflowPid1);
        assertEquals(sourceFile.getName(), sourceFile3.getName());
        assertEquals(sourceFile.getPath(), sourceFile3.getPath());
        assertEquals(sourceFile.getPid(), sourceFile3.getPid());
    }
}
