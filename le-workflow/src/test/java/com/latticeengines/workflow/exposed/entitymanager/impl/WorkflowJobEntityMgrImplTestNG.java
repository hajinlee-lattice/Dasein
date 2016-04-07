package com.latticeengines.workflow.exposed.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.user.WorkflowUser;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;

public class WorkflowJobEntityMgrImplTestNG extends WorkflowFunctionalTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    private String tenantId1;

    private String tenantId2;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tenantId1 = this.getClass().getSimpleName() + "1";
        tenantId2 = this.getClass().getSimpleName() + "2";
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);
        if (tenant2 != null) {
            tenantService.discardTenant(tenant2);
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);
        if (tenant2 != null) {
            tenantService.discardTenant(tenant2);
        }
    }

    @Test(groups = "functional")
    public void testCreateWorkflowJob() {
        Tenant tenant1 = new Tenant();
        tenant1.setId(tenantId1);
        tenant1.setName(tenantId1);
        tenantEntityMgr.create(tenant1);

        Tenant tenant2 = new Tenant();
        tenant2.setId(tenantId2);
        tenant2.setName(tenantId2);
        tenantEntityMgr.create(tenant2);

        WorkflowJob workflowJob1 = new WorkflowJob();
        workflowJob1.setApplicationId("application_00001");
        workflowJob1.setTenant(tenant1);
        workflowJob1.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob1.setInputContextValue("filename", "abc");
        workflowJobEntityMgr.create(workflowJob1);

        WorkflowJob workflowJob2 = new WorkflowJob();
        workflowJob2.setApplicationId("application_00002");
        workflowJob2.setTenant(tenant2);
        workflowJob2.setUserId("user2");
        workflowJobEntityMgr.create(workflowJob2);

        WorkflowJob workflowJob3 = new WorkflowJob();
        workflowJob3.setApplicationId("application_00003");
        workflowJob3.setTenant(tenant1);
        workflowJob3.setUserId("user3");
        workflowJobEntityMgr.create(workflowJob3);

        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByTenant(tenant1);
        assertEquals(workflowJobs.size(), 2);

        assertEquals(workflowJobs.get(0).getApplicationId(), "application_00001");
        assertEquals(workflowJobs.get(0).getUserId(), WorkflowUser.DEFAULT_USER.name());
        assertEquals(workflowJobs.get(0).getInputContext().get("filename"), "abc");
        assertEquals(workflowJobs.get(1).getApplicationId(), "application_00003");
        assertEquals(workflowJobs.get(1).getUserId(), "user3");

        WorkflowJob workflowJob4 = workflowJobEntityMgr.findByApplicationId("application_00003");
        assertEquals(workflowJobs.get(1).getTenantId(), workflowJob4.getTenantId());
        assertEquals(workflowJobs.get(1).getUserId(), workflowJob4.getUserId());

        workflowJobs = workflowJobEntityMgr.findByTenant(tenant2);
        assertEquals(workflowJobs.size(), 1);
        assertEquals(workflowJobs.get(0).getApplicationId(), "application_00002");
        assertEquals(workflowJobs.get(0).getUserId(), "user2");

    }
}
