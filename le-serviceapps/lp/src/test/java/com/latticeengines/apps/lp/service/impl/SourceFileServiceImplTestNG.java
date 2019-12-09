package com.latticeengines.apps.lp.service.impl;

import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

public class SourceFileServiceImplTestNG extends LPFunctionalTestNGBase {

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private TenantService tenantService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @BeforeClass(groups = { "functional" })
    public void setup() {
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
        sourceFileService.create(sourceFile);

        SourceFile sourceFile2 = sourceFileService.findByApplicationId(applicationId);
        assertEquals(sourceFile.getName(), sourceFile2.getName());
        assertEquals(sourceFile.getPath(), sourceFile2.getPath());
        assertEquals(sourceFile.getPid(), sourceFile2.getPid());
    }

    @Test(groups = "functional")
    public void testGetWorkflowPid() {
        String name = "SomeFileForWorkflowPid";
        Long workflowPid1 = Long.valueOf(111);
        String path = "path";
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName(name);
        sourceFile.setPath(path);
        sourceFile.setWorkflowPid(workflowPid1);
        sourceFileService.create(sourceFile);

        SourceFile sourceFile2 = sourceFileService.findByWorkflowPid(workflowPid1);
        assertEquals(sourceFile.getName(), sourceFile2.getName());
        assertEquals(sourceFile.getPath(), sourceFile2.getPath());
        assertEquals(sourceFile.getPid(), sourceFile2.getPid());
    }

    @Test(groups = "functional")
    public void testGetFakeApplicationId() {
        String name = "SomeFileForWorkflowPid2";
        Long workflowPid1 = Long.valueOf(111);
        FakeApplicationId appId = new FakeApplicationId(workflowPid1);
        String path = "path";
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName(name);
        sourceFile.setPath(path);
        sourceFile.setApplicationId(appId.toString());
        sourceFileService.create(sourceFile);

        SourceFile sourceFile2 = sourceFileService.findByWorkflowPid(workflowPid1);
        assertEquals(sourceFile.getName(), sourceFile2.getName());
        assertEquals(sourceFile.getPath(), sourceFile2.getPath());
        assertEquals(sourceFile.getPid(), sourceFile2.getPid());
    }
}
