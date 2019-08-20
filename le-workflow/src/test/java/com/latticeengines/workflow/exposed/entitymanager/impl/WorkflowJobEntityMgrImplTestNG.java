package com.latticeengines.workflow.exposed.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.user.WorkflowUser;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

public class WorkflowJobEntityMgrImplTestNG extends WorkflowTestNGBase {

    @Inject
    private TenantService tenantService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Value("${common.le.stack}")
    private String leStack;

    private String tenantId1;
    private String tenantId2;
    private Tenant tenant1;
    private Tenant tenant2;

    @BeforeClass(groups = "functional")
    @Override
    public void setup() throws Exception {
        tenantId1 = this.getClass().getSimpleName() + "1";
        tenantId2 = this.getClass().getSimpleName() + "2";
        tenant1 = tenantService.findByTenantId(tenantId1);
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        tenant2 = tenantService.findByTenantId(tenantId2);
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
    public void teardown() {
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        if (tenant2 != null) {
            tenantService.discardTenant(tenant2);
        }

        workflowJobEntityMgr.deleteAll();
    }

    @Test(groups = "functional")
    private void testEmptyInputIds() {
        List<WorkflowJob> emptyJobs = Collections.emptyList();
        // make sure the method can guard against empty/null input list
        Assert.assertEquals(workflowJobEntityMgr.findByWorkflowIds(Collections.emptyList()), emptyJobs);
        Assert.assertEquals(workflowJobEntityMgr.findByWorkflowIds(null), emptyJobs);
        Assert.assertEquals(workflowJobEntityMgr.findByWorkflowPids(Collections.emptyList()), emptyJobs);
        Assert.assertEquals(workflowJobEntityMgr.findByWorkflowPids(null), emptyJobs);
    }

    @Test(groups = "functional")
    public void testDeleteWorkflowJobByApplicationId() {
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);

        WorkflowJob workflowJob1 = new WorkflowJob();
        workflowJob1.setApplicationId("application_91000");
        workflowJob1.setTenant(tenant1);
        workflowJob1.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob1.setWorkflowId(91L);
        workflowJob1.setType("type1");
        workflowJob1.setParentJobId(null);
        workflowJob1.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.READY.name());
        workflowJobEntityMgr.create(workflowJob1);

        WorkflowJob workflowJob2 = new WorkflowJob();
        workflowJob2.setApplicationId("application_92000");
        workflowJob2.setTenant(tenant2);
        workflowJob2.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob2.setWorkflowId(92L);
        workflowJob2.setType("type1");
        workflowJob2.setParentJobId(null);
        workflowJob2.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.READY.name());
        workflowJobEntityMgr.create(workflowJob2);

        WorkflowJob result = workflowJobEntityMgr.deleteByApplicationId("application_99000");
        assertNull(result);

        WorkflowJob result1 = workflowJobEntityMgr.deleteByApplicationId("application_91000");
        assertEquals(result1.getPid(), workflowJob1.getPid());
        assertEquals(result1.getStatus(), workflowJob1.getStatus());

        MultiTenantContext.setTenant(tenant2);
        WorkflowJob result2 = workflowJobEntityMgr.deleteByApplicationId("application_92000");
        assertEquals(result2.getPid(), workflowJob2.getPid());
        assertEquals(result2.getStatus(), workflowJob2.getStatus());
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
        workflowJob1.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.READY.name());
        workflowJobEntityMgr.create(workflowJob1);

        WorkflowJob workflowJob11 = new WorkflowJob();
        workflowJob11.setApplicationId("application_10001");
        workflowJob11.setTenant(tenant1);
        workflowJob11.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob11.setWorkflowId(11L);
        workflowJob11.setType("type1");
        workflowJob11.setParentJobId(1L);
        workflowJob11.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.RUNNING.name());
        workflowJobEntityMgr.create(workflowJob11);

        WorkflowJob workflowJob12 = new WorkflowJob();
        workflowJob12.setApplicationId("application_10002");
        workflowJob12.setTenant(tenant1);
        workflowJob12.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob12.setWorkflowId(12L);
        workflowJob12.setType("type1");
        workflowJob12.setParentJobId(1L);
        workflowJob12.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED.name());
        workflowJobEntityMgr.create(workflowJob12);

        WorkflowJob workflowJob2 = new WorkflowJob();
        workflowJob2.setApplicationId("application_20000");
        workflowJob2.setTenant(tenant2);
        workflowJob2.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJob2.setWorkflowId(2L);
        workflowJob2.setType("type2");
        workflowJob2.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.FAILED.name());
        workflowJobEntityMgr.create(workflowJob2);

        MultiTenantContext.setTenant(tenant2);
        List<WorkflowJob> jobs = workflowJobEntityMgr.findAll();
        assertEquals(jobs.size(), 1);

        MultiTenantContext.setTenant(tenant1);
        jobs = workflowJobEntityMgr.findAll();
        assertEquals(jobs.size(), 3);
        List<String> applicationIds = jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList());
        assertTrue(applicationIds.contains("application_10000"));
        assertTrue(applicationIds.contains("application_10001"));
        assertTrue(applicationIds.contains("application_10002"));

        MultiTenantContext.setTenant(tenant2);
        jobs = workflowJobEntityMgr.findAll();
        assertEquals(jobs.size(), 1);
        assertTrue(jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList())
                .contains("application_20000"));

        MultiTenantContext.setTenant(null);
        jobs = workflowJobEntityMgr.findAll();
        assertTrue(jobs.size() >= 4);
        applicationIds = jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList());
        assertTrue(applicationIds.contains("application_10000"));
        assertTrue(applicationIds.contains("application_10001"));
        assertTrue(applicationIds.contains("application_10002"));
        assertTrue(applicationIds.contains("application_20000"));
    }

    @Test(groups = "functional", dependsOnMethods = "testFindAll")
    public void testFindByWorkflowIdOrTypesOrParentJobId() {
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);

        MultiTenantContext.setTenant(tenant1);
        List<WorkflowJob> jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                null, Collections.singletonList("type1"), null);
        assertEquals(jobs.size(), 3);
        List<String> applicationIds = jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList());
        assertTrue(applicationIds.contains("application_10000"));
        assertTrue(applicationIds.contains("application_10001"));
        assertTrue(applicationIds.contains("application_10002"));

        //Query by Status
        List<String> statuses = Arrays.asList(com.latticeengines.domain.exposed.workflow.JobStatus.RUNNING.name(),
                com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED.name());
        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                null, Collections.singletonList("type1"), statuses, null);
        assertEquals(jobs.size(), 2);

        List<Long> workflowIds = new ArrayList<>();
        workflowIds.add(11L);
        workflowIds.add(2L);
        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                workflowIds, null, null);
        assertEquals(jobs.size(), 1);
        assertEquals(jobs.get(0).getApplicationId(), "application_10001");

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                workflowIds, Collections.singletonList("type1"), null);
        assertEquals(jobs.size(), 1);
        assertEquals(jobs.get(0).getApplicationId(), "application_10001");

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                workflowIds, Collections.singletonList("type2"), null);
        assertEquals(jobs.size(), 0);

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                workflowIds, null, 1L);
        assertEquals(jobs.size(), 1);
        assertEquals(jobs.get(0).getApplicationId(), "application_10001");

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                workflowIds, null, -1L);
        assertEquals(jobs.size(), 0);

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                null, Collections.singletonList("type1"), 1L);
        assertEquals(jobs.size(), 2);
        assertEquals(jobs.get(0).getApplicationId(), "application_10001");
        assertEquals(jobs.get(1).getApplicationId(), "application_10002");

        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                null, Collections.singletonList("type1"), -1L);
        assertEquals(jobs.size(), 0);

        MultiTenantContext.setTenant(tenant2);
        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                null, Collections.singletonList("type2"), null);
        assertEquals(jobs.size(), 1);
        assertTrue(jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList())
                .contains("application_20000"));

        workflowIds.remove(0);
        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
                workflowIds, null, null);
        assertEquals(jobs.size(), 1);
        assertTrue(jobs.stream().map(WorkflowJob::getApplicationId).collect(Collectors.toList())
                .contains("application_20000"));

        workflowIds.clear();
        workflowIds.add(1L);
        jobs = workflowJobEntityMgr.findByWorkflowIdsOrTypesOrParentJobId(
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
        workflowJob1.setStack(leStack);
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
        workflowJob3.setStack(leStack);
        workflowJobEntityMgr.create(workflowJob3);

        MultiTenantContext.setTenant(tenant1);
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findAll();
        assertEquals(workflowJobs.size(), 2);

        assertEquals(workflowJobs.get(0).getApplicationId(), "application_00001");
        assertEquals(workflowJobs.get(0).getUserId(), WorkflowUser.DEFAULT_USER.name());
        assertEquals(workflowJobs.get(0).getInputContext().get("filename"), "abc");
        assertEquals(workflowJobs.get(0).getStack(), leStack);
        assertEquals(workflowJobs.get(1).getApplicationId(), "application_00003");
        assertEquals(workflowJobs.get(1).getUserId(), "user3");
        assertEquals(workflowJobs.get(1).getStack(), leStack);

        WorkflowJob workflowJob4 = workflowJobEntityMgr.findByApplicationId("application_00003");
        assertEquals(workflowJobs.get(1).getTenantId(), workflowJob4.getTenantId());
        assertEquals(workflowJobs.get(1).getUserId(), workflowJob4.getUserId());

        MultiTenantContext.setTenant(tenant2);
        workflowJobs = workflowJobEntityMgr.findAll();
        assertEquals(workflowJobs.size(), 1);
        assertEquals(workflowJobs.get(0).getApplicationId(), "application_00002");
        assertEquals(workflowJobs.get(0).getUserId(), "user2");
        assertNull(workflowJobs.get(0).getStack());
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
    public void testFindByTenantAndWorkflowPids() {
        Tenant tenant1 = tenantService.findByTenantId(tenantId1);
        MultiTenantContext.setTenant(tenant1);
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findAll();
        assertEquals(workflowJobs.size(), 2);

        List<Long> workflowPids = workflowJobs.stream().map(WorkflowJob::getPid).collect(Collectors.toList());
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowPid(workflowPids.get(0));
        assertNotNull(workflowJob);
        assertEquals(workflowJob.getApplicationId(), "application_00001");
        workflowJob = workflowJobEntityMgr.findByWorkflowPid(workflowPids.get(1));
        assertNotNull(workflowJob);
        assertEquals(workflowJob.getApplicationId(), "application_00003");

        workflowJobs = workflowJobEntityMgr.findByTenantAndWorkflowPids(tenant1, workflowPids);
        assertEquals(workflowJobs.size(), 2);

        workflowPids.remove(0);
        workflowJobs = workflowJobEntityMgr.findByTenantAndWorkflowPids(tenant1, workflowPids);
        assertEquals(workflowJobs.size(), 1);

        List<Long> nonExistWorkflowPids = Arrays.asList(
                ThreadLocalRandom.current().nextLong(),
                ThreadLocalRandom.current().nextLong(),
                ThreadLocalRandom.current().nextLong());
        workflowJobs = workflowJobEntityMgr.findByTenantAndWorkflowPids(tenant1, nonExistWorkflowPids);
        assertEquals(workflowJobs.size(), 0);
        workflowJobs = workflowJobEntityMgr.findByWorkflowIds(nonExistWorkflowPids);
        assertEquals(workflowJobs.size(), 0);
    }

    @Test(groups = "functional", dependsOnMethods = "testFindByTenantAndWorkflowPids", dataProvider = "provideQueryByClusterIDData")
    private void testFindByClusterIDAndTypesAndStatuses(WorkflowJob[] workflowJobs, ClusterIdQuery[] queries)
            throws Exception {
        try {
            Arrays.stream(workflowJobs).forEach(workflowJobEntityMgr::create);
            Thread.sleep(2000L);

            Arrays.stream(queries).forEach(this::queryAndVerify);
        } finally {
            Arrays.stream(workflowJobs).forEach(workflowJobEntityMgr::delete);
            Thread.sleep(2000L);
        }
    }

    private void queryAndVerify(ClusterIdQuery query) {
        String clusterId = query.clusterId;
        List<String> types = query.types;
        List<String> statuses = query.statuses;
        List<WorkflowJob> jobs = workflowJobEntityMgr.queryByClusterIDAndTypesAndStatuses(clusterId, types, statuses);
        Assert.assertNotNull(jobs);
        jobs.forEach(job -> {
            if (StringUtils.isNotBlank(clusterId)) {
                Assert.assertEquals(job.getEmrClusterId(), clusterId);
            }
            if (CollectionUtils.isNotEmpty(types)) {
                Assert.assertTrue(types.contains(job.getType()));
            }
            if (CollectionUtils.isNotEmpty(statuses)) {
                Assert.assertTrue(statuses.contains(job.getStatus()));
            }
        });
    }

    @Test(groups = "functional", dependsOnMethods = "testFindByClusterIDAndTypesAndStatuses")
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
        workflowJob2.setErrorDetails(new ErrorDetails(LedpCode.LEDP_32000, "abc", "abc"));
        workflowJobEntityMgr.updateErrorDetails(workflowJob2);
        WorkflowJob workflowJob3 = workflowJobEntityMgr.findByField("pid", workflowJob.getPid());
        assertEquals(workflowJob2.getErrorDetailsString(), workflowJob3.getErrorDetailsString());
        assertEquals(workflowJob3.getErrorCategory(), "User Error");
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

    @Test(groups = "functional", dependsOnMethods = "testUpdateOutput")
    public void testUpdateInput() {
        Tenant tenant2 = tenantService.findByTenantId(tenantId2);
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setApplicationId("application_000013");
        workflowJob.setTenant(tenant2);
        workflowJob.setUserId(WorkflowUser.DEFAULT_USER.name());
        workflowJobEntityMgr.create(workflowJob);
        WorkflowJob workflowJob2 = workflowJobEntityMgr.findByField("pid", workflowJob.getPid());
        assertNull(workflowJob2.getOutputContextString());
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put(WorkflowContextConstants.Inputs.ACTION_IDS, "[111,222]");
        workflowJob2.setInputContext(inputMap);
        workflowJobEntityMgr.updateInput(workflowJob2);
        WorkflowJob workflowJob3 = workflowJobEntityMgr.findByField("pid", workflowJob.getPid());
        assertEquals(workflowJob2.getInputContextString(), workflowJob3.getInputContextString());
    }

    @DataProvider(name = "provideQueryByClusterIDData")
    private Object[][] provideQueryClusterIdData() {
        return new Object[][]{ //
                { //
                        new WorkflowJob[]{ //
                                newJob("c1", "PA", "RUNNING"), //
                                newJob("c1", "PA", "FAILED"), //
                                newJob("c1", "PA", "PENDING"), //
                                newJob("c1", "PA", "COMPLETED"), //
                                newJob("c1", "PA", "RUNNING"), //
                                newJob("c1", "PA", "RUNNING"), //

                                newJob("c1", "Import", "RUNNING"), //
                                newJob("c1", "Import", "RUNNING"), //
                                newJob("c1", "Import", "FAILED"), //
                                newJob("c1", "Import", "COMPLETED"), //

                                newJob("c2", "PA", "RUNNING"), //
                                newJob("c2", "PA", "FAILED"), //
                                newJob("c2", "PA", "PENDING"), //
                                newJob("c2", "PA", "COMPLETED"), //

                                newJob("c2", "Import", "RUNNING"), //
                                newJob("c2", "Import", "PENDING"), //

                                newJob(null, "PA", "PENDING"), //
                                newJob(null, "PA", "PENDING"), //
                        }, //
                        new ClusterIdQuery[]{ //
                                new ClusterIdQuery("c1", null, null), //
                                new ClusterIdQuery("c1", Collections.singletonList("PA"), null), //
                                new ClusterIdQuery("c1", Collections.singletonList("PA"),
                                        Arrays.asList("RUNNING", "PENDING")), //
                                new ClusterIdQuery("c1", null, Arrays.asList("RUNNING", "PENDING")), //
                                new ClusterIdQuery(null, Collections.singletonList("PA"),
                                        Arrays.asList("RUNNING", "PENDING")), //
                                new ClusterIdQuery(null, null, Arrays.asList("RUNNING", "PENDING")), //
                                new ClusterIdQuery(null, null, null), //
                                new ClusterIdQuery("c1", Arrays.asList("PA", "Import", "Nothing"),
                                        Arrays.asList("RUNNING", "PENDING")), //
                                new ClusterIdQuery(null, Arrays.asList("PA", "Import", "Nothing"),
                                        Arrays.asList("RUNNING", "PENDING")), //
                                new ClusterIdQuery(null, Collections.singletonList("PA"), null), //
                        } //
                }
                // TODO add more test cases
        };
    }

    private class ClusterIdQuery {
        String clusterId;
        List<String> types;
        List<String> statuses;

        ClusterIdQuery(String clusterId, List<String> types, List<String> statuses) {
            this.clusterId = clusterId;
            this.types = types;
            this.statuses = statuses;
        }
    }

    private WorkflowJob newJob(String clusterId, String type, String status) {
        WorkflowJob job = new WorkflowJob();
        job.setApplicationId("app_" + UUID.randomUUID().toString());
        job.setTenant(tenant1);
        job.setType(type);
        job.setEmrClusterId(clusterId);
        job.setStatus(status);
        job.setUserId(WorkflowUser.DEFAULT_USER.name());
        return job;
    }
}
