package com.latticeengines.workflow.exposed.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.user.WorkflowUser;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

public class WorkflowJobEntityMgrImplTestNG extends WorkflowTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    private String tenantId1;

    private String tenantId2;

    @BeforeClass(groups = "functional")
    @Override
    public void setup() {
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

        tenant1 = new Tenant();
        tenant1.setId(tenantId1);
        tenant1.setName(tenantId1);
        tenantEntityMgr.create(tenant1);

        tenant2 = new Tenant();
        tenant2.setId(tenantId2);
        tenant2.setName(tenantId2);
        tenantEntityMgr.create(tenant2);
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
    public void testFindAll() {
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);

        WorkflowJob workflowJob1 = new WorkflowJob();
        workflowJob1.setApplicationId("application_10000");
        workflowJob1.setTenant(tenant1);
        workflowJob1.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob1.setWorkflowId(1L);
        workflowJob1.setType("type1");
        workflowJob1.setParentJobId(null);
        workflowJobEntityMgr.create(workflowJob1);

        WorkflowJob workflowJob11 = new WorkflowJob();
        workflowJob11.setApplicationId("application_10001");
        workflowJob11.setTenant(tenant1);
        workflowJob11.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob11.setWorkflowId(11L);
        workflowJob11.setType("type1");
        workflowJob11.setParentJobId(1L);
        workflowJobEntityMgr.create(workflowJob11);

        WorkflowJob workflowJob12 = new WorkflowJob();
        workflowJob12.setApplicationId("application_10002");
        workflowJob12.setTenant(tenant1);
        workflowJob12.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob12.setWorkflowId(12L);
        workflowJob12.setType("type1");
        workflowJob12.setParentJobId(1L);
        workflowJobEntityMgr.create(workflowJob12);

        WorkflowJob workflowJob2 = new WorkflowJob();
        workflowJob2.setApplicationId("application_20000");
        workflowJob2.setTenant(tenant2);
        workflowJob2.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob2.setWorkflowId(2L);
        workflowJob2.setType("type2");
        workflowJobEntityMgr.create(workflowJob2);

        List<WorkflowJob> jobs = workflowJobEntityMgr.findAll();
        assertEquals(jobs.size(), 4);

        MultiTenantContext.setTenant(tenant1);
        jobs = workflowJobEntityMgr.findAllWithFilter();
        assertEquals(jobs.size(), 3);
        List<String> applicationIds = jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList());
        assertTrue(applicationIds.contains("application_10000"));
        assertTrue(applicationIds.contains("application_10001"));
        assertTrue(applicationIds.contains("application_10002"));

        MultiTenantContext.setTenant(tenant2);
        jobs = workflowJobEntityMgr.findAllWithFilter();
        assertEquals(jobs.size(), 1);
        assertTrue(jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList())
                .contains("application_20000"));
    }

    @Test(groups = "functional", dependsOnMethods = "testFindAll")
    public void testFindByWorkflowIdOrTypesOrParentJobId() {
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);

        MultiTenantContext.setTenant(tenant1);
        List<WorkflowJob> jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                null, Collections.singletonList("type1"), null);
        assertEquals(jobs.size(), 3);
        List<String> applicationIds = jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList());
        assertTrue(applicationIds.contains("application_10000"));
        assertTrue(applicationIds.contains("application_10001"));
        assertTrue(applicationIds.contains("application_10002"));

        List<Long> workflowIds = new ArrayList<>();
        workflowIds.add(11L);
        workflowIds.add(2L);
        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                workflowIds, null, null);
        assertEquals(jobs.size(), 1);
        assertEquals(jobs.get(0).getApplicationId(), "application_10001");

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                workflowIds, Collections.singletonList("type1"), null);
        assertEquals(jobs.size(), 1);
        assertEquals(jobs.get(0).getApplicationId(), "application_10001");

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                workflowIds, Collections.singletonList("type2"), null);
        assertEquals(jobs.size(), 0);

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                workflowIds, null, 1L);
        assertEquals(jobs.size(), 1);
        assertEquals(jobs.get(0).getApplicationId(), "application_10001");

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                workflowIds, null, -1L);
        assertEquals(jobs.size(), 0);

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                null, Collections.singletonList("type1"), 1L);
        assertEquals(jobs.size(), 2);
        assertEquals(jobs.get(0).getApplicationId(), "application_10001");
        assertEquals(jobs.get(1).getApplicationId(), "application_10002");

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                null, Collections.singletonList("type1"), -1L);
        assertEquals(jobs.size(), 0);

        MultiTenantContext.setTenant(tenant2);
        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                null, Collections.singletonList("type2"), null);
        assertEquals(jobs.size(), 1);
        assertTrue(jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList())
                .contains("application_20000"));

        workflowIds.remove(0);
        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                workflowIds, null, null);
        assertEquals(jobs.size(), 1);
        assertTrue(jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList())
                .contains("application_20000"));

        workflowIds.clear();
        workflowIds.add(1L);
        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobIdWithFilter(
                workflowIds, Collections.singletonList("type2"), null);
        assertEquals(jobs.size(), 0);

        workflowJobEntityMgr.deleteAll();
    }

    @Test(groups = "functional", dependsOnMethods = "testFindByWorkflowIdOrTypesOrParentJobId")
    public void testCreateWorkflowJob() {
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);
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

    @Test(groups = "functional", dependsOnMethods = "testCreateWorkflowJob")
    public void testUpdateStatusFromYarn() {
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        WorkflowJob workflowJob5 = new WorkflowJob();
        workflowJob5.setApplicationId("application_00005");
        workflowJob5.setTenant(tenant1);
        workflowJob5.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob5.setInputContextValue("filename", "abc");
        workflowJobEntityMgr.create(workflowJob5);

        workflowJob5.setWorkflowId(5L);
        workflowJobEntityMgr.registerWorkflowId(workflowJob5);
        workflowJob5 = workflowJobEntityMgr.findByApplicationId(workflowJob5.getApplicationId());
        assertEquals(workflowJob5.getWorkflowId(), new Long(5L));

        JobStatus yarnJobStatus = new JobStatus();
        yarnJobStatus.setStatus(FinalApplicationStatus.UNDEFINED);
        yarnJobStatus.setState(YarnApplicationState.ACCEPTED);
        workflowJob5.setWorkflowId(null);
        workflowJobEntityMgr.updateStatusFromYarn(workflowJob5, yarnJobStatus);
        workflowJob5 = workflowJobEntityMgr.findByApplicationId(workflowJob5.getApplicationId());
        assertEquals(workflowJob5.getWorkflowId(), new Long(5L));
        assertEquals(workflowJob5.getStatus(), com.latticeengines.domain.exposed.workflow.JobStatus.PENDING.name());

        yarnJobStatus.setStatus(FinalApplicationStatus.FAILED);
        yarnJobStatus.setState(YarnApplicationState.FAILED);
        yarnJobStatus.setStartTime(10000L);
        workflowJob5.setWorkflowId(null);
        workflowJobEntityMgr.updateStatusFromYarn(workflowJob5, yarnJobStatus);
        workflowJob5 = workflowJobEntityMgr.findByApplicationId(workflowJob5.getApplicationId());
        assertEquals(workflowJob5.getWorkflowId(), new Long(5L));
        assertEquals(workflowJob5.getStatus(), com.latticeengines.domain.exposed.workflow.JobStatus.FAILED.name());
        assertEquals(workflowJob5.getStartTimeInMillis(), new Long(10000L));

        yarnJobStatus.setStatus(FinalApplicationStatus.UNDEFINED);
        yarnJobStatus.setState(YarnApplicationState.RUNNING);
        yarnJobStatus.setStartTime(20000L);
        workflowJob5.setWorkflowId(null);
        workflowJobEntityMgr.updateStatusFromYarn(workflowJob5, yarnJobStatus);
        workflowJob5 = workflowJobEntityMgr.findByApplicationId(workflowJob5.getApplicationId());
        assertEquals(workflowJob5.getWorkflowId(), new Long(5L));
        assertEquals(workflowJob5.getStatus(), com.latticeengines.domain.exposed.workflow.JobStatus.RUNNING.name());
        assertEquals(workflowJob5.getStartTimeInMillis(), new Long(20000L));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateWorkflowJob")
    public void testFindByTenantAndWorkflowIds() {
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByTenant(tenant1);
        assertEquals(workflowJobs.size(), 2);

        workflowJobs.get(0).setWorkflowId(100L);
        workflowJobs.get(1).setWorkflowId(200L);
        workflowJobEntityMgr.update(workflowJobs.get(0));
        workflowJobEntityMgr.update(workflowJobs.get(1));
        List<Long> workflowIds = new ArrayList<>();
        workflowIds.add(100L);
        workflowJobs = workflowJobEntityMgr.findByTenantAndWorkflowIds(tenant1, workflowIds);
        assertEquals(workflowJobs.size(), 1);

        workflowIds.add(200L);
        workflowJobs = workflowJobEntityMgr.findByTenantAndWorkflowIds(tenant1, workflowIds);
        assertEquals(workflowJobs.size(), 2);

        workflowJobs = workflowJobEntityMgr.findByWorkflowIds(workflowIds);
        assertEquals(workflowJobs.size(), 2);

        List<Long> nonExistWorkflowIds = new ArrayList<>();
        nonExistWorkflowIds.add(777L);
        nonExistWorkflowIds.add(888L);
        nonExistWorkflowIds.add(999L);
        workflowJobs = workflowJobEntityMgr.findByTenantAndWorkflowIds(tenant1, nonExistWorkflowIds);
        assertEquals(workflowJobs.size(), 0);
        workflowJobs = workflowJobEntityMgr.findByWorkflowIds(nonExistWorkflowIds);
        assertEquals(workflowJobs.size(), 0);
    }

    @Test(groups = "functional", dependsOnMethods = "testFindByTenantAndWorkflowIds")
    public void testUpdateReport() {
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setApplicationId("application_000010");
        workflowJob.setTenant(tenant2);
        workflowJob.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJobEntityMgr.create(workflowJob);
        WorkflowJob workflowJob2 = workflowJobEntityMgr.findByField("pid", workflowJob.getPid());
        assertNull(workflowJob2.getReportContextString());
        workflowJob2.setReportContextString("abc");
        workflowJobEntityMgr.updateReport(workflowJob2);
        WorkflowJob workflowJob3 = workflowJobEntityMgr.findByField("pid", workflowJob.getPid());
        assertEquals(workflowJob2.getReportContextString(), workflowJob3.getReportContextString());
    }

    @Test(groups = "functional", dependsOnMethods = "testUpdateReport")
    public void testUpdateError() {
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setApplicationId("application_000011");
        workflowJob.setTenant(tenant2);
        workflowJob.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJobEntityMgr.create(workflowJob);
        WorkflowJob workflowJob2 = workflowJobEntityMgr.findByField("pid", workflowJob.getPid());
        assertNull(workflowJob2.getErrorDetailsString());
        workflowJob2.setErrorDetailsString("abc");
        workflowJobEntityMgr.updateErrorDetails(workflowJob2);
        WorkflowJob workflowJob3 = workflowJobEntityMgr.findByField("pid", workflowJob.getPid());
        assertEquals(workflowJob2.getErrorDetailsString(), workflowJob3.getErrorDetailsString());
    }

    @Test(groups = "functional", dependsOnMethods = "testUpdateError")
    public void testUpdateOutput() {
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setApplicationId("application_000012");
        workflowJob.setTenant(tenant2);
        workflowJob.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJobEntityMgr.create(workflowJob);
        WorkflowJob workflowJob2 = workflowJobEntityMgr.findByField("pid", workflowJob.getPid());
        assertNull(workflowJob2.getOutputContextString());
        workflowJob2.setOutputContextString("abc");
        workflowJobEntityMgr.updateOutput(workflowJob2);
        WorkflowJob workflowJob3 = workflowJobEntityMgr.findByField("pid", workflowJob.getPid());
        assertEquals(workflowJob2.getOutputContextString(), workflowJob3.getOutputContextString());
    }
}
