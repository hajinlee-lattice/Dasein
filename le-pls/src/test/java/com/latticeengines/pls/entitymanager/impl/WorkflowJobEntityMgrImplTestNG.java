package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.latticeengines.domain.exposed.pls.WorkflowJob;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;

public class WorkflowJobEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        Tenant tenant2 = tenantService.findByTenantId("TENANT2");
        if (tenant2 != null) {
            tenantService.discardTenant(tenant2);
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        Tenant tenant2 = tenantService.findByTenantId("TENANT2");
        if (tenant2 != null) {
            tenantService.discardTenant(tenant2);
        }
    }

    @Test(groups = "functional")
    public void testCreateWorkflowJob() {
        Tenant tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);

        Tenant tenant2 = new Tenant();
        tenant2.setId("TENANT2");
        tenant2.setName("TENANT2");
        tenantEntityMgr.create(tenant2);

        setupSecurityContext(tenant1, "user1_1");
        WorkflowJob workflowJob1 = new WorkflowJob();
        workflowJob1.setYarnAppId("application_00001");
        workflowJobEntityMgr.create(workflowJob1);

        setupSecurityContext(tenant2, "user2_1");
        WorkflowJob workflowJob2 = new WorkflowJob();
        workflowJob2.setYarnAppId("application_00002");
        workflowJobEntityMgr.create(workflowJob2);

        setupSecurityContext(tenant1, "user1_2");
        WorkflowJob workflowJob3 = new WorkflowJob();
        workflowJob3.setYarnAppId("application_00003");
        workflowJobEntityMgr.create(workflowJob3);

        setupSecurityContext(tenant1);
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findAll();
        assertEquals(workflowJobs.size(), 2);

        assertEquals(workflowJobs.get(0).getYarnAppId(), "application_00001");
        assertEquals(workflowJobs.get(0).getUser(), "user1_1");
        assertEquals(workflowJobs.get(1).getYarnAppId(), "application_00003");
        assertEquals(workflowJobs.get(1).getUser(), "user1_2");

        setupSecurityContext(tenant2);
        workflowJobs = workflowJobEntityMgr.findAll();
        assertEquals(workflowJobs.size(), 1);
        assertEquals(workflowJobs.get(0).getYarnAppId(), "application_00002");
        assertEquals(workflowJobs.get(0).getUser(), "user2_1");

    }
}
