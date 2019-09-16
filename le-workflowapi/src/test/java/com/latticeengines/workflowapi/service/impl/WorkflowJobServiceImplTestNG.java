package com.latticeengines.workflowapi.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.MapExecutionContextDao;
import org.springframework.batch.core.repository.dao.MapJobExecutionDao;
import org.springframework.batch.core.repository.dao.MapJobInstanceDao;
import org.springframework.batch.core.repository.dao.MapStepExecutionDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.service.impl.WorkflowServiceImpl;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
import com.latticeengines.workflowapi.service.WorkflowThrottlingService;

public class WorkflowJobServiceImplTestNG extends WorkflowApiFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImplTestNG.class);

    private static final String TEST_WF_NAME = "testWorkflowName";

    @Inject
    TenantEntityMgr tenantEntityMgr;

    @Mock
    private WorkflowThrottlingService workflowThrottlingService;

    private Map<Long, Long> workflowIds = new HashMap<>();
    private Map<Long, Long> workflowPids = new HashMap<>();

    private JobInstanceDao jobInstanceDao;

    private JobExecutionDao jobExecutionDao;

    private StepExecutionDao stepExecutionDao;

    private ExecutionContextDao executionContextDao;

    private WorkflowJob workflowJob1;
    private WorkflowJob workflowJob11;
    private WorkflowJob workflowJob12;
    private WorkflowJob workflowJob13;
    private WorkflowJob workflowJob2;
    private WorkflowJob workflowJob3;
    private WorkflowJob workflowJob31;
    private WorkflowJob workflowJobNoWorkflowId;

    private WorkflowJob shouldEnqueuedWorkflowJob;
    private WorkflowConfiguration shouldEnqueuedWorkflowConfig;

    private WorkflowJobUpdate jobUpdate1;
    private WorkflowJobUpdate jobUpdate11;
    private WorkflowJobUpdate jobUpdate12;
    private WorkflowJobUpdate jobUpdate13;
    private WorkflowJobUpdate jobUpdate2;
    private WorkflowJobUpdate jobUpdate3;
    private WorkflowJobUpdate jobUpdate31;

    private long heartbeatThreshold;
    private long currentTime;
    private long jobUpdateCreateTime;
    private long successTime;
    private long failedTime;

    private CustomerSpace customerSpace3;
    private Tenant tenant3;

    private Function<WorkflowJob, Job> workflowJobToJobMapper = (WorkflowJob workflowJob) -> {
        if (workflowJob == null) {
            return null;
        }

        Job job = new Job();
        job.setId(workflowJob.getWorkflowId());
        job.setParentId(workflowJob.getParentJobId());
        job.setApplicationId(workflowJob.getApplicationId());
        job.setJobStatus(JobStatus.fromString(workflowJob.getStatus()));
        job.setInputs(workflowJob.getInputContext());
        job.setJobType(workflowJob.getType());
        job.setSteps(new ArrayList<>());
        return job;
    };

    @BeforeClass(groups = "functional")
    public void setupThrottlingWorkflow() throws NoSuchFieldException {
        MockitoAnnotations.initMocks(this);
        ReflectionTestUtils.setField(workflowJobService, "workflowThrottlingService", workflowThrottlingService);

        shouldEnqueuedWorkflowConfig = new WorkflowConfiguration();
        shouldEnqueuedWorkflowConfig.setCustomerSpace(WFAPITEST_CUSTOMERSPACE);
        shouldEnqueuedWorkflowConfig.setWorkflowName(TEST_WF_NAME);
    }

    @PostConstruct
    public void init() {
        heartbeatThreshold = TimeUnit.MILLISECONDS.convert(2L, TimeUnit.MINUTES);
        currentTime = System.currentTimeMillis();
        successTime = currentTime - heartbeatThreshold;
        failedTime = currentTime - 12 * heartbeatThreshold;
        jobUpdateCreateTime = currentTime - 20 * heartbeatThreshold;
        log.info(String.format("heartbeatThreshold=%s, currentTime=%s, successTime=%s, failedTime=%s",
                heartbeatThreshold, currentTime, successTime, failedTime));
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        workflowJobEntityMgr.delete(workflowJob1);
        workflowJobEntityMgr.delete(workflowJob11);
        workflowJobEntityMgr.delete(workflowJob12);
        workflowJobEntityMgr.delete(workflowJob13);
        workflowJobEntityMgr.delete(workflowJob2);
        workflowJobEntityMgr.delete(workflowJob3);
        workflowJobEntityMgr.delete(workflowJob31);

        workflowJobUpdateEntityMgr.delete(jobUpdate1);
        workflowJobUpdateEntityMgr.delete(jobUpdate11);
        workflowJobUpdateEntityMgr.delete(jobUpdate12);
        workflowJobUpdateEntityMgr.delete(jobUpdate13);
        workflowJobUpdateEntityMgr.delete(jobUpdate2);
        workflowJobUpdateEntityMgr.delete(jobUpdate3);
        workflowJobUpdateEntityMgr.delete(jobUpdate31);

        tenantEntityMgr.delete(tenant3);
    }

    @Test(groups = "functional", dataProvider = "enqueueWorkflowDataProvider")
    public void testEnqueueWorkflow(boolean expectBackPressure) {
        when(workflowThrottlingService.queueLimitReached(any(), any(), any(), any())).thenReturn(expectBackPressure);
        when(workflowThrottlingService.shouldEnqueueWorkflow(any(), any(), any())).thenReturn(true);
        if (expectBackPressure) {
            Assert.assertThrows(IllegalStateException.class, () -> workflowJobService.enqueueWorkflow(tenant.getId(), shouldEnqueuedWorkflowConfig, null));
        } else {
            ApplicationId workflowSubmission = workflowJobService.enqueueWorkflow(tenant.getId(), shouldEnqueuedWorkflowConfig, null);
            Assert.assertNotNull(workflowSubmission);
            long workflowJobPid = FakeApplicationId.toWorkflowJobPid(workflowSubmission.toString());
            shouldEnqueuedWorkflowJob = workflowJobEntityMgr.findByWorkflowPid(workflowJobPid);
            Assert.assertNotNull(shouldEnqueuedWorkflowJob);
            Assert.assertEquals(shouldEnqueuedWorkflowJob.getType(), shouldEnqueuedWorkflowConfig.getWorkflowName());
            Assert.assertEquals(shouldEnqueuedWorkflowJob.getWorkflowConfiguration().getCustomerSpace(), shouldEnqueuedWorkflowConfig.getCustomerSpace());
            Assert.assertEquals(shouldEnqueuedWorkflowJob.getStatus(), JobStatus.ENQUEUED.name());
            Assert.assertEquals(shouldEnqueuedWorkflowJob.getStack(), CamilleEnvironment.getDivision());
            Assert.assertEquals(shouldEnqueuedWorkflowJob.getApplicationId(), new FakeApplicationId(workflowJobPid).toString());
            Assert.assertNull(shouldEnqueuedWorkflowJob.getEmrClusterId());
            workflowJobEntityMgr.delete(shouldEnqueuedWorkflowJob);
        }
        when(workflowThrottlingService.shouldEnqueueWorkflow(any(), any(), any())).thenReturn(false);
    }

    @DataProvider(name = "enqueueWorkflowDataProvider")
    public Object[][] enqueueWorkflowDataProvider() {
        // with or without back pressure
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(groups = "functional")
    public void testDeleteWorkflowJobByApplicationId() {
        WorkflowJob workflowJob1 = new WorkflowJob();
        workflowJob1.setApplicationId("application_1549391605495_91000");
        workflowJob1.setWorkflowId(91L);
        workflowJob1.setTenant(tenant);
        workflowJob1.setType("type1");
        workflowJob1.setParentJobId(null);
        workflowJob1.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.READY.name());
        workflowJobEntityMgr.create(workflowJob1);
        WorkflowJobUpdate jobUpdate1 = new WorkflowJobUpdate();
        jobUpdate1.setWorkflowPid(workflowJob1.getPid());
        jobUpdate1.setCreateTime(System.currentTimeMillis());
        jobUpdate1.setLastUpdateTime(System.currentTimeMillis());
        workflowJobUpdateEntityMgr.create(jobUpdate1);

        WorkflowJob workflowJob2 = new WorkflowJob();
        workflowJob2.setApplicationId("application_1549391605495_92000");
        workflowJob2.setWorkflowId(92L);
        workflowJob2.setTenant(tenant);
        workflowJob2.setType("type1");
        workflowJob2.setParentJobId(null);
        workflowJob2.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.READY.name());
        workflowJobEntityMgr.create(workflowJob2);

        WorkflowJob result = workflowJobService.deleteWorkflowJobByApplicationId(
                WFAPITEST_CUSTOMERSPACE.toString(), "987654");
        Assert.assertNull(result);

        result = workflowJobService.deleteWorkflowJobByApplicationId(
                WFAPITEST_CUSTOMERSPACE.toString(), "application_1549391605495_91000");
        Assert.assertEquals(result.getPid(), workflowJob1.getPid());

        result = workflowJobService.deleteWorkflowJobByApplicationId(
                WFAPITEST_CUSTOMERSPACE.toString(), "application_1549391605495_92000");
        Assert.assertEquals(result.getPid(), workflowJob2.getPid());

        Assert.assertNull(workflowJobService.getJobByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowJob1.getPid(), Boolean.FALSE));
        Assert.assertNull(workflowJobService.getJobByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowJob2.getPid(), Boolean.FALSE));
    }

    @Test(groups = "functional")
    public void testDeleteWorkflowJobs() {
        Long currentTime = System.currentTimeMillis();
        WorkflowJob workflowJob1 = new WorkflowJob();
        workflowJob1.setApplicationId("application_1549391605495_91000");
        workflowJob1.setWorkflowId(91L);
        workflowJob1.setTenant(tenant);
        workflowJob1.setType("type1");
        workflowJob1.setParentJobId(null);
        workflowJob1.setStartTimeInMillis(currentTime);
        workflowJob1.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.READY.name());
        workflowJobEntityMgr.create(workflowJob1);
        WorkflowJobUpdate jobUpdate1 = new WorkflowJobUpdate();
        jobUpdate1.setWorkflowPid(workflowJob1.getPid());
        jobUpdate1.setCreateTime(currentTime);
        jobUpdate1.setLastUpdateTime(currentTime);
        workflowJobUpdateEntityMgr.create(jobUpdate1);

        WorkflowJob workflowJob2 = new WorkflowJob();
        workflowJob2.setApplicationId("application_1549391605495_92000");
        workflowJob2.setWorkflowId(92L);
        workflowJob2.setTenant(tenant);
        workflowJob2.setType("type2");
        workflowJob2.setParentJobId(null);
        workflowJob2.setStartTimeInMillis(currentTime);
        workflowJob2.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.READY.name());
        workflowJobEntityMgr.create(workflowJob2);

        WorkflowJob workflowJob3 = new WorkflowJob();
        workflowJob3.setApplicationId(null);
        workflowJob3.setWorkflowId(null);
        workflowJob3.setTenant(tenant);
        workflowJob3.setType("type2");
        workflowJob3.setParentJobId(null);
        workflowJob3.setStartTimeInMillis(currentTime);
        workflowJob3.setStatus(com.latticeengines.domain.exposed.workflow.JobStatus.READY.name());
        workflowJobEntityMgr.create(workflowJob3);

        List<WorkflowJob> result = workflowJobService.deleteWorkflowJobs(
                WFAPITEST_CUSTOMERSPACE.toString(), "type1",
                currentTime - TimeUnit.MILLISECONDS.convert(10L, TimeUnit.MINUTES),
                currentTime - TimeUnit.MILLISECONDS.convert(5L, TimeUnit.MINUTES));
        Assert.assertTrue(result.isEmpty());

        result = workflowJobService.deleteWorkflowJobs(
                WFAPITEST_CUSTOMERSPACE.toString(), "type1",
                currentTime - TimeUnit.MILLISECONDS.convert(2L, TimeUnit.MINUTES),
                currentTime + TimeUnit.MILLISECONDS.convert(2L, TimeUnit.MINUTES));
        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0).getApplicationId(), workflowJob1.getApplicationId());

        Assert.assertNull(workflowJobService.getJobByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowJob1.getPid(), Boolean.FALSE));
        Assert.assertNotNull(workflowJobService.getJobByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowJob2.getPid(), Boolean.FALSE));
        Assert.assertNotNull(workflowJobService.getJobByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowJob3.getPid(), Boolean.FALSE));

        result = workflowJobService.deleteWorkflowJobs(
                WFAPITEST_CUSTOMERSPACE.toString(), "type2",
                currentTime - TimeUnit.MILLISECONDS.convert(2L, TimeUnit.MINUTES),
                currentTime + TimeUnit.MILLISECONDS.convert(2L, TimeUnit.MINUTES));
        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), 2);
        Assert.assertEquals(result.get(0).getApplicationId(), workflowJob2.getApplicationId());
        Assert.assertNull(result.get(1).getApplicationId());

        Assert.assertNull(workflowJobService.getJobByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowJob2.getPid(), Boolean.FALSE));
        Assert.assertNull(workflowJobService.getJobByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowJob3.getPid(), Boolean.FALSE));
    }

    @Test(groups = "functional")
    public void testGetJobStatus() {
        createWorkflowJobs();
        setupLEJobExecutionRetriever();
        mockWorkflowContainerService();

        JobStatus status = workflowJobService.getJobStatusByWorkflowId(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10000L));
        Assert.assertEquals(status, JobStatus.RUNNING);
        status = workflowJobService.getJobStatusByWorkflowPid(WFAPITEST_CUSTOMERSPACE.toString(), workflowPids.get(1L));
        Assert.assertEquals(status, JobStatus.RUNNING);

        status = workflowJobService.getJobStatusByWorkflowId(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10001L));
        Assert.assertEquals(status, JobStatus.PENDING);
        status = workflowJobService.getJobStatusByWorkflowPid(WFAPITEST_CUSTOMERSPACE.toString(), workflowPids.get(11L));
        Assert.assertEquals(status, JobStatus.PENDING);

        status = workflowJobService.getJobStatusByWorkflowId(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10002L));
        Assert.assertEquals(status, JobStatus.COMPLETED);
        status = workflowJobService.getJobStatusByWorkflowPid(WFAPITEST_CUSTOMERSPACE.toString(), workflowPids.get(12L));
        Assert.assertEquals(status, JobStatus.COMPLETED);

        status = workflowJobService.getJobStatusByWorkflowId(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10003L));
        Assert.assertEquals(status, JobStatus.FAILED);
        status = workflowJobService.getJobStatusByWorkflowPid(WFAPITEST_CUSTOMERSPACE.toString(), workflowPids.get(13L));
        Assert.assertEquals(status, JobStatus.FAILED);

        status = workflowJobService.getJobStatusByWorkflowId(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(20000L));
        Assert.assertEquals(status, JobStatus.FAILED);
        status = workflowJobService.getJobStatusByWorkflowPid(WFAPITEST_CUSTOMERSPACE.toString(), workflowPids.get(2L));
        Assert.assertEquals(status, JobStatus.FAILED);

        status = workflowJobService.getJobStatusByWorkflowId(customerSpace3.toString(), workflowIds.get(30000L));
        Assert.assertEquals(status, JobStatus.PENDING);
        status = workflowJobService.getJobStatusByWorkflowPid(customerSpace3.toString(), workflowPids.get(3L));
        Assert.assertEquals(status, JobStatus.PENDING);

        // although job (workflowId=30001L, workflowPid=31L) has bad heartbeat,
        // it should not be FAILED because its yarn status is RUNNING
        status = workflowJobService.getJobStatusByWorkflowId(customerSpace3.toString(), workflowIds.get(30001L));
        Assert.assertEquals(status, JobStatus.RUNNING);
        status = workflowJobService.getJobStatusByWorkflowPid(customerSpace3.toString(), workflowPids.get(31L));
        Assert.assertEquals(status, JobStatus.RUNNING);

        List<Long> testWorkflowIds1 = new ArrayList<>();
        testWorkflowIds1.add(workflowIds.get(10000L));
        testWorkflowIds1.add(workflowIds.get(20000L));
        testWorkflowIds1.add(workflowIds.getOrDefault(90000L, 90000L));
        List<JobStatus> statuses = workflowJobService.getJobStatusByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(),
                testWorkflowIds1);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.RUNNING);
        Assert.assertEquals(statuses.get(1), JobStatus.FAILED);

        List<Long> testWorkflowPids1 = new ArrayList<>();
        testWorkflowPids1.add(workflowPids.get(1L));
        testWorkflowPids1.add(workflowPids.get(2L));
        testWorkflowPids1.add(workflowPids.getOrDefault(9L, 9L));
        statuses = workflowJobService.getJobStatusByWorkflowPids(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowPids1);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.RUNNING);
        Assert.assertEquals(statuses.get(1), JobStatus.FAILED);

        List<Long> testWorkflowIds2 = new ArrayList<>();
        testWorkflowIds2.add(workflowIds.get(10000L));
        testWorkflowIds2.add(workflowIds.get(10002L));
        statuses = workflowJobService.getJobStatusByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds2);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.RUNNING);
        Assert.assertEquals(statuses.get(1), JobStatus.COMPLETED);

        MultiTenantContext.setTenant(tenant);
        statuses = workflowJobService.getJobStatusByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.RUNNING);
        Assert.assertEquals(statuses.get(1), JobStatus.FAILED);

        statuses = workflowJobService.getJobStatusByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds2);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.RUNNING);
        Assert.assertEquals(statuses.get(1), JobStatus.COMPLETED);

        // job (workflowId=30001L, workflowPid=31L) should not be FAILED because its yarn status is RUNNING
        status = workflowJobService.getJobStatusByWorkflowId(customerSpace3.toString(), workflowIds.get(30001L));
        Assert.assertNotNull(status);
        Assert.assertEquals(status, JobStatus.RUNNING);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetJobStatus", expectedExceptions = {
            RuntimeException.class}, expectedExceptionsMessageRegExp = "No tenant found with id .*")
    public void testGetJobs() {
        mockWorkflowService();
        setupLEJobExecutionRetriever();

        List<Job> jobs = workflowJobService.getJobsByCustomerSpace(WFAPITEST_CUSTOMERSPACE.toString(), false);
        Assert.assertEquals(jobs.size(), 6);
        List<String> applicationIds = jobs.stream().map(Job::getApplicationId).collect(Collectors.toList());
        Assert.assertTrue(applicationIds.contains("application_1549391605495_10000"));
        Assert.assertTrue(applicationIds.contains("application_1549391605495_10001"));
        Assert.assertTrue(applicationIds.contains("application_1549391605495_10002"));
        Assert.assertTrue(applicationIds.contains("application_1549391605495_10003"));
        Assert.assertTrue(applicationIds.contains("application_1549391605495_20000"));

        jobs = workflowJobService.getJobsByCustomerSpace(customerSpace3.toString(), false);
        Assert.assertEquals(jobs.size(), 2);
        applicationIds = jobs.stream().map(Job::getApplicationId).collect(Collectors.toList());
        Assert.assertTrue(applicationIds.contains("application_1549391605495_30000"));
        Assert.assertTrue(applicationIds.contains("application_1549391605495_30001"));
        // Apply Status Filter
        jobs = workflowJobService.getJobsByWorkflowIds(customerSpace3.toString(), null, null,
                Collections.singletonList(JobStatus.PENDING.toString()), true, false, null);
        Assert.assertEquals(jobs.size(), 1);

        // CustomerSpace is required now. Remove null customerSpace tests.

        List<Long> workflowExecutionIds = new ArrayList<>(workflowIds.values());
        jobs = workflowJobService.getJobsByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), workflowExecutionIds,
                null, true, null, null);
        Assert.assertEquals(jobs.size(), 5);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_1549391605495_10000");
        Assert.assertEquals(jobs.get(0).getSteps().size(), 2);
        Assert.assertEquals(jobs.get(0).getSteps().get(0).getJobStepType(), "step1_2");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_1549391605495_10001");
        Assert.assertEquals(jobs.get(1).getSteps().size(), 3);
        Assert.assertEquals(jobs.get(1).getSteps().get(1).getJobStepType(), "step11_2");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_1549391605495_10002");
        Assert.assertEquals(jobs.get(2).getSteps().size(), 1);
        Assert.assertEquals(jobs.get(2).getSteps().get(0).getJobStepType(), "step12_1");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_1549391605495_10003");
        Assert.assertEquals(jobs.get(3).getSteps().size(), 3);
        Assert.assertEquals(jobs.get(3).getSteps().get(2).getJobStepType(), "step13_1");
        Assert.assertEquals(jobs.get(4).getApplicationId(), "application_1549391605495_20000");
        Assert.assertEquals(jobs.get(4).getSteps().size(), 9);
        Assert.assertEquals(jobs.get(4).getSteps().get(1).getJobStepType(), "step2_8");
        // Apply Status Filter
        jobs = workflowJobService.getJobsByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), workflowExecutionIds,
                null, Collections.singletonList(JobStatus.FAILED.toString()), true, null, null);
        Assert.assertEquals(jobs.size(), 3);

        List<Long> pids = new ArrayList<>(workflowPids.values());
        jobs = workflowJobService.getJobsByWorkflowPids(WFAPITEST_CUSTOMERSPACE.toString(), pids,
                null, true, null, null);
        Assert.assertEquals(jobs.size(), 6);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_1549391605495_10000");
        Assert.assertEquals(jobs.get(0).getSteps().size(), 2);
        Assert.assertEquals(jobs.get(0).getSteps().get(0).getJobStepType(), "step1_2");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_1549391605495_10001");
        Assert.assertEquals(jobs.get(1).getSteps().size(), 3);
        Assert.assertEquals(jobs.get(1).getSteps().get(1).getJobStepType(), "step11_2");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_1549391605495_10002");
        Assert.assertEquals(jobs.get(2).getSteps().size(), 1);
        Assert.assertEquals(jobs.get(2).getSteps().get(0).getJobStepType(), "step12_1");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_1549391605495_10003");
        Assert.assertEquals(jobs.get(3).getSteps().size(), 3);
        Assert.assertEquals(jobs.get(3).getSteps().get(2).getJobStepType(), "step13_1");
        Assert.assertEquals(jobs.get(4).getApplicationId(), "application_1549391605495_20000");
        Assert.assertEquals(jobs.get(4).getSteps().size(), 9);
        Assert.assertEquals(jobs.get(4).getSteps().get(1).getJobStepType(), "step2_8");

        List<String> workflowExecutionTypes = new ArrayList<>();
        workflowExecutionTypes.add("application_1549391605495_30000");
        workflowExecutionTypes.add("application_1549391605495_30001");
        jobs = workflowJobService.getJobsByWorkflowIds(customerSpace3.toString(), null,
                workflowExecutionTypes, true, null, null);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_1549391605495_30000");
        Assert.assertEquals(jobs.get(0).getSteps().size(), 1);
        Assert.assertEquals(jobs.get(0).getSteps().get(0).getJobStepType(), "step3_1");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_1549391605495_30001");
        Assert.assertEquals(jobs.get(1).getSteps().size(), 4);
        Assert.assertEquals(jobs.get(1).getSteps().get(3).getJobStepType(), "step31_1");

        MultiTenantContext.setTenant(tenant3);
        workflowExecutionTypes.add("application_1549391605495_10000");
        jobs = workflowJobService.getJobsByWorkflowIds(null, null, workflowExecutionTypes,
                true, null, null);
        Assert.assertEquals(jobs.size(), 3);
        jobs.forEach(job -> {
            Assert.assertTrue(workflowExecutionTypes.contains(job.getApplicationId()));
            Assert.assertTrue(job.getSteps().size() > 0);
        });

        jobs = workflowJobService.getJobsByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), workflowExecutionIds,
                Collections.singletonList("application_1549391605495_10002"), true, false, -1L);
        Assert.assertEquals(jobs.size(), 1);
        Assert.assertEquals(jobs.get(0).getJobType(), "application_1549391605495_10002");
        Assert.assertEquals(jobs.get(0).getSteps().size(), 1);
        jobs = workflowJobService.getJobsByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), workflowExecutionIds,
                Collections.singletonList("application_1549391605495_20000"), false, false, -1L);
        Assert.assertEquals(jobs.size(), 1);
        Assert.assertEquals(jobs.get(0).getJobType(), "application_1549391605495_20000");
        Assert.assertNull(jobs.get(0).getSteps());

        workflowJobService.getJobsByWorkflowIds(StringUtils.EMPTY, null, null, false,
                false, 0L);
        jobs = workflowJobService.getJobsByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), null,
                null, false, false, 0L);
        Assert.assertEquals(jobs.size(), 5);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_1549391605495_10000");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_1549391605495_10001");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_1549391605495_10002");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_1549391605495_10003");
        Assert.assertEquals(jobs.get(4).getApplicationId(), "application_1549391605495_20000");

        List<Long> testWorkflowIds1 = new ArrayList<>();
        testWorkflowIds1.add(workflowIds.get(10000L));
        testWorkflowIds1.add(workflowIds.get(10001L));
        testWorkflowIds1.add(workflowIds.get(10002L));
        testWorkflowIds1.add(workflowIds.get(20000L));
        testWorkflowIds1.add(workflowIds.getOrDefault(90000L, 90000L));
        jobs = workflowJobService.getJobsByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1,
                null, false, false, 0L);
        Assert.assertEquals(jobs.size(), 4);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_1549391605495_10000");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_1549391605495_10001");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_1549391605495_10002");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_1549391605495_20000");

        List<String> testTypes = new ArrayList<>();
        testTypes.add("application_1549391605495_10001");
        jobs = workflowJobService.getJobsByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1, testTypes,
                false, false,
                0L);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_1549391605495_10001");
        Assert.assertNull(jobs.get(0).getSteps());

        jobs = workflowJobService.getJobsByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1, testTypes,
                true, false,
                0L);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_1549391605495_10001");
        Assert.assertNotNull(jobs.get(0).getSteps());

        Long parentJobId = workflowIds.get(10000L);
        jobs = workflowJobService.getJobsByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1,
                null, true, true,
                parentJobId);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_1549391605495_10001");
        Assert.assertNotNull(jobs.get(0).getSteps());
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_1549391605495_10002");
        Assert.assertNotNull(jobs.get(1).getSteps());
    }

    @Test(groups = "functional", dependsOnMethods = "testGetJobStatus")
    public void testGetStepNames() {
        mockWorkflowService();
        setupLEJobExecutionRetriever();

        Long workflowPid = workflowPids.get(1L);
        List<String> stepNames = workflowJobService.getStepNames(WFAPITEST_CUSTOMERSPACE.toString(), workflowPid);
        Assert.assertNotNull(stepNames);
        Assert.assertEquals(stepNames.size(), 2);
        Assert.assertEquals(stepNames.get(0), "step1_2");
        Assert.assertEquals(stepNames.get(1), "step1_1");

        workflowPid = workflowPids.getOrDefault(9L, 9L);
        Assert.assertNull(workflowJobService.getStepNames(WFAPITEST_CUSTOMERSPACE.toString(), workflowPid));
    }

    @Test(groups = "functional", dependsOnMethods = {"testGetJobStatus", "testGetJobs"})
    public void testUpdateParentJobId() {
        List<Long> testWorkflowIds = new ArrayList<>();
        testWorkflowIds.add(workflowIds.get(10001L));
        testWorkflowIds.add(workflowIds.get(10002L));
        testWorkflowIds.add(workflowIds.get(10003L));
        testWorkflowIds.add(workflowIds.getOrDefault(90000L, 90000L));
        MultiTenantContext.setTenant(tenant);
        WorkflowJob workflowjob0 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(0));
        WorkflowJob workflowjob1 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(1));
        WorkflowJob workflowjob2 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(2));
        WorkflowJob workflowjob3 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(3));
        Assert.assertEquals(workflowjob0.getParentJobId(), workflowIds.get(10000L));
        Assert.assertEquals(workflowjob1.getParentJobId(), workflowIds.get(10000L));
        Assert.assertEquals(workflowjob2.getParentJobId(), workflowIds.get(10000L));
        Assert.assertNull(workflowjob3);
        Long parentJobId = 20000L;
        workflowJobService.updateParentJobIdByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds, parentJobId);
        workflowjob0 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(0));
        workflowjob1 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(1));
        workflowjob2 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(2));
        Assert.assertEquals(workflowjob0.getParentJobId(), parentJobId);
        Assert.assertEquals(workflowjob1.getParentJobId(), parentJobId);
        Assert.assertEquals(workflowjob2.getParentJobId(), parentJobId);
    }

    @Test(groups = "functional", dependsOnMethods = {"testUpdateParentJobId"})
    private void testUpdateStatusAfterRetry() throws Exception {
        long workflowId = workflowIds.get(30001L);

        MultiTenantContext.setTenant(null);
        WorkflowJob originalJob = workflowJobEntityMgr.findByWorkflowId(workflowId);
        Assert.assertNotNull(originalJob);

        workflowJobService.updateWorkflowStatusAfterRetry(customerSpace3.toString(), workflowId);

        try {
            Thread.sleep(1000L);
            MultiTenantContext.setTenant(null);
            WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(workflowId);
            Assert.assertNotNull(job);
            Assert.assertEquals(job.getStatus(), JobStatus.RETRIED.name(), "Job status should be retried after update");
        } finally {
            // restore to original state
            workflowJobEntityMgr.updateWorkflowJob(originalJob);
        }
    }

    @Test(groups = "functional", dependsOnMethods = "testGetJobStatus")
    public void testMultiTenantFilter() {
        List<Long> testWorkflowIds1 = new ArrayList<>();
        testWorkflowIds1.add(workflowIds.get(30000L));
        testWorkflowIds1.add(workflowIds.get(30001L));
        List<JobStatus> status1 = workflowJobService.getJobStatusByWorkflowIds(customerSpace3.toString(), testWorkflowIds1);
        Assert.assertEquals(status1.size(), 2);
        Assert.assertEquals(status1.get(0), JobStatus.PENDING);
        Assert.assertEquals(status1.get(1), JobStatus.FAILED);

        List<Long> testWorkflowPids1 = new ArrayList<>();
        testWorkflowPids1.add(workflowPids.get(3L));
        testWorkflowPids1.add(workflowPids.get(31L));
        status1 = workflowJobService.getJobStatusByWorkflowPids(customerSpace3.toString(), testWorkflowPids1);
        Assert.assertEquals(status1.size(), 2);
        Assert.assertEquals(status1.get(0), JobStatus.PENDING);
        Assert.assertEquals(status1.get(1), JobStatus.FAILED);

        List<Long> testWorkflowIds2 = new ArrayList<>();
        testWorkflowIds2.add(workflowIds.get(10000L));
        testWorkflowIds2.add(workflowIds.get(10002L));
        testWorkflowIds2.add(workflowIds.get(30001L));
        List<JobStatus> status2 = workflowJobService.getJobStatusByWorkflowIds(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds2);
        Assert.assertEquals(status2.size(), 2);
        Assert.assertEquals(status2.get(0), JobStatus.RUNNING);
        Assert.assertEquals(status2.get(1), JobStatus.COMPLETED);

        Long parentJobId = 30000L;
        WorkflowJob workflowJob0 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds2.get(0));
        WorkflowJob workflowJob1 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds2.get(1));
        Assert.assertNull(workflowJob0.getParentJobId());
        Assert.assertEquals(workflowJob1.getParentJobId(), workflowIds.get(10000L));
        WorkflowJob workflowJob2 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds2.get(2));
        Assert.assertNull(workflowJob2);

        workflowJobService.updateParentJobIdByWorkflowIds(customerSpace3.toString(), testWorkflowIds2, parentJobId);
        workflowJob0 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds2.get(0));
        workflowJob1 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds2.get(1));
        workflowJob2 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds2.get(2));
        Assert.assertNull(workflowJob0);
        Assert.assertNull(workflowJob1);
        Assert.assertEquals(workflowJob2.getParentJobId(), parentJobId);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetJobStatus")
    public void testGetJobExecutionAndGetExecutionContext() {
        JobExecution jobExecution1a = workflowJobService.getJobExecutionByWorkflowId(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10000L));
        Assert.assertEquals(jobExecution1a.getStartTime().getTime(), 1530327693564L);
        Assert.assertEquals(jobExecution1a.getLastUpdated().getTime(), 1530327694564L);
        Assert.assertEquals(jobExecution1a.getJobParameters().getString("jobName"), "application_1549391605495_10000");
        JobExecution jobExecution1b = workflowJobService.getJobExecutionByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowPids.get(1L));
        Assert.assertEquals(jobExecution1b, jobExecution1a);
        JobExecution jobExecution1c = workflowJobService.getJobExecutionByApplicationId(
                WFAPITEST_CUSTOMERSPACE.toString(), "application_1549391605495_10000");
        Assert.assertEquals(jobExecution1c, jobExecution1a);

        ExecutionContext executionContext1a = workflowJobService.getExecutionContextByWorkflowId(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10000L));
        Assert.assertEquals(executionContext1a.getString("CDL_ACTIVE_VERSION"), "Blue");
        Assert.assertEquals(executionContext1a.getString("TRANSFORM_PIPELINE_VERSION"),
                "2018-10-23_06-26-34_UTC");
        Assert.assertEquals(executionContext1a.getString("CUSTOMER_SPACE"),
                "LETest1000000000000.LETest1000000000000.Production");
        Assert.assertEquals(executionContext1a.getLong("PA_TIMESTAMP"), 1530327696564L);
        ExecutionContext executionContext1b = workflowJobService.getExecutionContextByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowPids.get(1L));
        Assert.assertEquals(executionContext1b, executionContext1a);
        ExecutionContext executionContext1c = workflowJobService.getExecutionContextByApplicationId(
                WFAPITEST_CUSTOMERSPACE.toString(), "application_1549391605495_10000");
        Assert.assertEquals(executionContext1c, executionContext1a);

        JobExecution jobExecution11a = workflowJobService.getJobExecutionByWorkflowId(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10001L));
        Assert.assertEquals(jobExecution11a.getStartTime().getTime(), 1530327693564L);
        Assert.assertEquals(jobExecution11a.getLastUpdated().getTime(), 1530327694564L);
        Assert.assertEquals(jobExecution11a.getJobParameters().getString("jobName"), "application_1549391605495_10001");
        JobExecution jobExecution11b = workflowJobService.getJobExecutionByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowPids.get(11L));
        Assert.assertEquals(jobExecution11b, jobExecution11a);
        JobExecution jobExecution11c = workflowJobService.getJobExecutionByApplicationId(
                WFAPITEST_CUSTOMERSPACE.toString(), "application_1549391605495_10001");
        Assert.assertEquals(jobExecution11c, jobExecution11a);

        ExecutionContext executionContext11a = workflowJobService.getExecutionContextByWorkflowId(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10001L));
        Assert.assertEquals(executionContext11a.getString("CDL_ACTIVE_VERSION"), "Green");
        Assert.assertEquals(executionContext11a.getString("TRANSFORM_PIPELINE_VERSION"),
                "2018-10-10_10-10-10_UTC");
        Assert.assertEquals(executionContext11a.getString("CUSTOMER_SPACE"),
                "LETest1000000000001.LETest1000000000001.QA");
        Assert.assertEquals(executionContext11a.getLong("PA_TIMESTAMP"), 1539166210000L);
        ExecutionContext executionContext11b = workflowJobService.getExecutionContextByWorkflowPid(
                WFAPITEST_CUSTOMERSPACE.toString(), workflowPids.get(11L));
        Assert.assertEquals(executionContext11b, executionContext11a);
        ExecutionContext executionContext11c = workflowJobService.getExecutionContextByApplicationId(
                WFAPITEST_CUSTOMERSPACE.toString(), "application_1549391605495_10001");
        Assert.assertEquals(executionContext11c, executionContext11a);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetJobStatus")
    public void testWorkflowJobNoWorkflowId() {
        try {
            List<Job> jobs = workflowJobService.getJobsByCustomerSpace(WFAPITEST_CUSTOMERSPACE.toString(), false);
            Assert.assertEquals(jobs.size(), 6);
        } catch (Exception exc) {
            Assert.fail("Shoud not throw exception.");
        }
    }

    private void createWorkflowJobs() {
        jobInstanceDao = new MapJobInstanceDao();
        jobExecutionDao = new MapJobExecutionDao();
        stepExecutionDao = new MapStepExecutionDao();
        executionContextDao = new MapExecutionContextDao();

        Map<String, String> inputContext = new HashMap<>();
        inputContext.put(WorkflowContextConstants.Inputs.JOB_TYPE, "testJob");

        String applicationId = "application_1549391605495_10000";
        Map<String, JobParameter> parameters1 = new HashMap<>();
        parameters1.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters1 = new JobParameters(parameters1);
        JobInstance jobInstance1 = jobInstanceDao.createJobInstance(applicationId, jobParameters1);
        JobExecution jobExecution1 = new JobExecution(jobInstance1, jobParameters1, null);
        jobExecution1.setStartTime(new Date(1530327693564L));
        jobExecution1.setLastUpdated(new Date(1530327694564L));
        ExecutionContext executionContext1 = new ExecutionContext();
        executionContext1.putString("CDL_ACTIVE_VERSION", "Blue");
        executionContext1.putString("TRANSFORM_PIPELINE_VERSION", "2018-10-23_06-26-34_UTC");
        executionContext1.putString("CUSTOMER_SPACE", "LETest1000000000000.LETest1000000000000.Production");
        executionContext1.putLong("PA_TIMESTAMP", 1530327696564L);
        jobExecution1.setExecutionContext(executionContext1);
        jobExecutionDao.saveJobExecution(jobExecution1);
        executionContextDao.saveExecutionContext(jobExecution1);
        List<StepExecution> steps = new ArrayList<>();
        steps.add(new StepExecution("step1_1", jobExecution1));
        steps.add(new StepExecution("step1_2", jobExecution1));
        stepExecutionDao.saveStepExecutions(steps);
        workflowJob1 = new WorkflowJob();
        workflowJob1.setApplicationId(applicationId);
        workflowJob1.setStartTimeInMillis(1530327693564L);
        workflowJob1.setInputContext(inputContext);
        workflowJob1.setTenant(tenant);
        workflowJob1.setStatus(
                JobStatus.fromString(FinalApplicationStatus.UNDEFINED.name(), YarnApplicationState.RUNNING).name());
        workflowJob1.setWorkflowId(jobExecution1.getJobId());
        workflowJob1.setType(jobInstance1.getJobName());
        workflowJob1.setParentJobId(null);
        workflowJobEntityMgr.create(workflowJob1);
        workflowIds.put(10000L, jobExecution1.getJobId());
        workflowPids.put(1L, workflowJob1.getPid());
        jobUpdate1 = new WorkflowJobUpdate();
        jobUpdate1.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob1.getWorkflowId()).getPid());
        jobUpdate1.setLastUpdateTime(successTime);
        jobUpdate1.setCreateTime(jobUpdateCreateTime);
        workflowJobUpdateEntityMgr.create(jobUpdate1);

        applicationId = "application_1549391605495_10001";
        Map<String, JobParameter> parameters11 = new HashMap<>();
        parameters11.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters11 = new JobParameters(parameters11);
        JobInstance jobInstance11 = jobInstanceDao.createJobInstance(applicationId, jobParameters11);
        JobExecution jobExecution11 = new JobExecution(jobInstance11, jobParameters11, null);
        jobExecution11.setStartTime(new Date(1530327693564L));
        jobExecution11.setLastUpdated(new Date(1530327694564L));
        ExecutionContext executionContext11 = new ExecutionContext();
        executionContext11.putString("CDL_ACTIVE_VERSION", "Green");
        executionContext11.putString("TRANSFORM_PIPELINE_VERSION", "2018-10-10_10-10-10_UTC");
        executionContext11.putString("CUSTOMER_SPACE", "LETest1000000000001.LETest1000000000001.QA");
        executionContext11.putLong("PA_TIMESTAMP", 1539166210000L);
        jobExecution11.setExecutionContext(executionContext11);
        jobExecutionDao.saveJobExecution(jobExecution11);
        executionContextDao.saveExecutionContext(jobExecution11);
        steps.clear();
        steps.add(new StepExecution("step11_1", jobExecution11));
        steps.add(new StepExecution("step11_2", jobExecution11));
        steps.add(new StepExecution("step11_3", jobExecution11));
        stepExecutionDao.saveStepExecutions(steps);
        workflowJob11 = new WorkflowJob();
        workflowJob11.setApplicationId(applicationId);
        workflowJob11.setStartTimeInMillis(1530327693564L);
        workflowJob11.setInputContext(inputContext);
        workflowJob11.setTenant(tenant);
        workflowJob11.setStatus(JobStatus.fromString(FinalApplicationStatus.UNDEFINED.name()).name());
        workflowJob11.setWorkflowId(jobExecution11.getJobId());
        workflowJob11.setType(jobInstance11.getJobName());
        workflowJob11.setParentJobId(workflowIds.get(10000L));
        workflowJobEntityMgr.create(workflowJob11);
        workflowIds.put(10001L, jobExecution11.getJobId());
        workflowPids.put(11L, workflowJob11.getPid());
        jobUpdate11 = new WorkflowJobUpdate();
        jobUpdate11.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob11.getWorkflowId()).getPid());
        jobUpdate11.setLastUpdateTime(failedTime);
        jobUpdate11.setCreateTime(jobUpdateCreateTime);
        workflowJobUpdateEntityMgr.create(jobUpdate11);

        applicationId = "application_1549391605495_10002";
        Map<String, JobParameter> parameters12 = new HashMap<>();
        parameters12.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters12 = new JobParameters(parameters12);
        JobInstance jobInstance12 = jobInstanceDao.createJobInstance(applicationId, jobParameters12);
        JobExecution jobExecution12 = new JobExecution(jobInstance12, jobParameters12, null);
        jobExecutionDao.saveJobExecution(jobExecution12);
        steps.clear();
        steps.add(new StepExecution("step12_1", jobExecution12));
        stepExecutionDao.saveStepExecutions(steps);
        workflowJob12 = new WorkflowJob();
        workflowJob12.setApplicationId(applicationId);
        workflowJob12.setStartTimeInMillis(1530327693564L);
        workflowJob12.setInputContext(inputContext);
        workflowJob12.setTenant(tenant);
        workflowJob12.setStatus(JobStatus.fromString(FinalApplicationStatus.SUCCEEDED.name()).name());
        workflowJob12.setWorkflowId(jobExecution12.getJobId());
        workflowJob12.setType(jobInstance12.getJobName());
        workflowJob12.setParentJobId(workflowIds.get(10000L));
        workflowJobEntityMgr.create(workflowJob12);
        workflowIds.put(10002L, jobExecution12.getJobId());
        workflowPids.put(12L, workflowJob12.getPid());
        jobUpdate12 = new WorkflowJobUpdate();
        jobUpdate12.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob12.getWorkflowId()).getPid());
        jobUpdate12.setLastUpdateTime(failedTime);
        jobUpdate12.setCreateTime(jobUpdateCreateTime);
        workflowJobUpdateEntityMgr.create(jobUpdate12);

        applicationId = "application_1549391605495_10003";
        Map<String, JobParameter> parameters13 = new HashMap<>();
        parameters13.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters13 = new JobParameters(parameters13);
        JobInstance jobInstance13 = jobInstanceDao.createJobInstance(applicationId, jobParameters13);
        JobExecution jobExecution13 = new JobExecution(jobInstance13, jobParameters13, null);
        jobExecutionDao.saveJobExecution(jobExecution13);
        steps.clear();
        steps.add(new StepExecution("step13_1", jobExecution13));
        steps.add(new StepExecution("step13_2", jobExecution13));
        steps.add(new StepExecution("step13_3", jobExecution13));
        stepExecutionDao.saveStepExecutions(steps);
        workflowJob13 = new WorkflowJob();
        workflowJob13.setApplicationId(applicationId);
        workflowJob13.setStartTimeInMillis(1530327693564L);
        workflowJob13.setInputContext(inputContext);
        workflowJob13.setTenant(tenant);
        workflowJob13.setStatus(JobStatus.fromString(FinalApplicationStatus.FAILED.name()).name());
        workflowJob13.setWorkflowId(jobExecution13.getJobId());
        workflowJob13.setType(jobInstance13.getJobName());
        workflowJob13.setParentJobId(workflowIds.get(10000L));
        workflowJobEntityMgr.create(workflowJob13);
        workflowIds.put(10003L, jobExecution13.getJobId());
        workflowPids.put(13L, workflowJob13.getPid());
        jobUpdate13 = new WorkflowJobUpdate();
        jobUpdate13.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob13.getWorkflowId()).getPid());
        jobUpdate13.setLastUpdateTime(failedTime);
        jobUpdate13.setCreateTime(jobUpdateCreateTime);
        workflowJobUpdateEntityMgr.create(jobUpdate13);

        applicationId = "application_1549391605495_20000";
        Map<String, JobParameter> parameters2 = new HashMap<>();
        parameters2.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters2 = new JobParameters(parameters2);
        JobInstance jobInstance2 = jobInstanceDao.createJobInstance(applicationId, jobParameters2);
        JobExecution jobExecution2 = new JobExecution(jobInstance2, jobParameters2, null);
        jobExecutionDao.saveJobExecution(jobExecution2);
        steps.clear();
        steps.add(new StepExecution("step2_1", jobExecution2));
        steps.add(new StepExecution("step2_2", jobExecution2));
        steps.add(new StepExecution("step2_3", jobExecution2));
        steps.add(new StepExecution("step2_4", jobExecution2));
        steps.add(new StepExecution("step2_5", jobExecution2));
        steps.add(new StepExecution("step2_6", jobExecution2));
        steps.add(new StepExecution("step2_7", jobExecution2));
        steps.add(new StepExecution("step2_8", jobExecution2));
        steps.add(new StepExecution("step2_9", jobExecution2));
        stepExecutionDao.saveStepExecutions(steps);
        workflowJob2 = new WorkflowJob();
        workflowJob2.setApplicationId(applicationId);
        workflowJob2.setStartTimeInMillis(1530327693564L);
        workflowJob2.setInputContext(inputContext);
        workflowJob2.setTenant(tenant);
        workflowJob2.setStatus(JobStatus.fromString(FinalApplicationStatus.KILLED.name()).name());
        workflowJob2.setWorkflowId(jobExecution2.getJobId());
        workflowJob2.setType(jobInstance2.getJobName());
        workflowJobEntityMgr.create(workflowJob2);
        workflowIds.put(20000L, jobExecution2.getJobId());
        workflowPids.put(2L, workflowJob2.getPid());
        jobUpdate2 = new WorkflowJobUpdate();
        jobUpdate2.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob2.getWorkflowId()).getPid());
        jobUpdate2.setLastUpdateTime(successTime);
        jobUpdate2.setCreateTime(jobUpdateCreateTime);
        workflowJobUpdateEntityMgr.create(jobUpdate2);

        createTenant();
        applicationId = "application_1549391605495_30000";
        Map<String, JobParameter> parameters3 = new HashMap<>();
        parameters3.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters3 = new JobParameters(parameters3);
        JobInstance jobInstance3 = jobInstanceDao.createJobInstance(applicationId, jobParameters3);
        JobExecution jobExecution3 = new JobExecution(jobInstance3, jobParameters3, null);
        jobExecutionDao.saveJobExecution(jobExecution3);
        steps.clear();
        steps.add(new StepExecution("step3_1", jobExecution3));
        stepExecutionDao.saveStepExecutions(steps);
        workflowJob3 = new WorkflowJob();
        workflowJob3.setApplicationId(applicationId);
        workflowJob3.setStartTimeInMillis(1530327693564L);
        workflowJob3.setInputContext(inputContext);
        workflowJob3.setTenant(tenant3);
        workflowJob3.setStatus(JobStatus.fromString(FinalApplicationStatus.UNDEFINED.name()).name());
        workflowJob3.setWorkflowId(jobExecution3.getJobId());
        workflowJob3.setType(jobInstance3.getJobName());
        workflowJobEntityMgr.create(workflowJob3);
        workflowIds.put(30000L, jobExecution3.getJobId());
        workflowPids.put(3L, workflowJob3.getPid());
        jobUpdate3 = new WorkflowJobUpdate();
        jobUpdate3.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob3.getWorkflowId()).getPid());
        jobUpdate3.setLastUpdateTime(successTime);
        jobUpdate3.setCreateTime(jobUpdateCreateTime);
        workflowJobUpdateEntityMgr.create(jobUpdate3);

        applicationId = "application_1549391605495_30001";
        Map<String, JobParameter> parameters31 = new HashMap<>();
        parameters31.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters31 = new JobParameters(parameters31);
        JobInstance jobInstance31 = jobInstanceDao.createJobInstance(applicationId, jobParameters31);
        JobExecution jobExecution31 = new JobExecution(jobInstance31, jobParameters31, null);
        jobExecutionDao.saveJobExecution(jobExecution31);
        steps.clear();
        steps.add(new StepExecution("step31_1", jobExecution31));
        steps.add(new StepExecution("step31_2", jobExecution31));
        steps.add(new StepExecution("step31_3", jobExecution31));
        steps.add(new StepExecution("step31_4", jobExecution31));
        stepExecutionDao.saveStepExecutions(steps);
        workflowJob31 = new WorkflowJob();
        workflowJob31.setApplicationId(applicationId);
        workflowJob31.setStartTimeInMillis(1530327693564L);
        workflowJob31.setInputContext(inputContext);
        workflowJob31.setTenant(tenant3);
        workflowJob31.setStatus(
                JobStatus.fromString(FinalApplicationStatus.UNDEFINED.name(), YarnApplicationState.RUNNING).name());
        workflowJob31.setWorkflowId(jobExecution31.getJobId());
        workflowJob31.setType(jobInstance31.getJobName());
        workflowJobEntityMgr.create(workflowJob31);
        workflowIds.put(30001L, jobExecution31.getJobId());
        workflowPids.put(31L, workflowJob31.getPid());
        jobUpdate31 = new WorkflowJobUpdate();
        jobUpdate31.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob31.getWorkflowId()).getPid());
        jobUpdate31.setLastUpdateTime(failedTime);
        jobUpdate31.setCreateTime(jobUpdateCreateTime);
        workflowJobUpdateEntityMgr.create(jobUpdate31);

        applicationId = "application_1549391605495_90001";
        workflowJobNoWorkflowId = new WorkflowJob();
        workflowJobNoWorkflowId.setTenant(tenant);
        workflowJobNoWorkflowId.setApplicationId(applicationId);
        workflowJobNoWorkflowId.setStartTimeInMillis(1530327693564L);
        workflowJobNoWorkflowId.setStatus(JobStatus.RUNNING.name());
        workflowJobEntityMgr.create(workflowJobNoWorkflowId);
        workflowPids.put(91L, workflowJobNoWorkflowId.getPid());
    }

    private void createTenant() {
        customerSpace3 = CustomerSpace.parse("test3.test3.test3");
        tenant3 = tenantEntityMgr.findByTenantId(customerSpace3.toString());
        if (tenant3 != null) {
            tenantEntityMgr.delete(tenant3);
        }
        tenant3 = new Tenant();
        tenant3.setId(customerSpace3.toString());
        tenant3.setName(customerSpace3.toString());
        tenantEntityMgr.create(tenant3);
        MultiTenantContext.setTenant(tenant3);
    }

    private void setupLEJobExecutionRetriever() {
        LEJobExecutionRetriever leJobExecutionRetriever = new LEJobExecutionRetriever();
        leJobExecutionRetriever.setJobInstanceDao(jobInstanceDao);
        leJobExecutionRetriever.setJobExecutionDao(jobExecutionDao);
        leJobExecutionRetriever.setStepExecutionDao(stepExecutionDao);
        leJobExecutionRetriever.setExecutionContextDao(executionContextDao);
        ((WorkflowJobServiceImpl) workflowJobService).setLeJobExecutionRetriever(leJobExecutionRetriever);
    }

    private void mockWorkflowService() {
        WorkflowServiceImpl workflowService = Mockito.mock(WorkflowServiceImpl.class);
        Mockito.when(workflowService.getStepNames(Mockito.any(WorkflowExecutionId.class))).thenAnswer(invocation -> {
            Long workflowId = ((WorkflowExecutionId) invocation.getArguments()[0]).getId();
            Job job = workflowJobToJobMapper.apply(workflowJobEntityMgr.findByWorkflowId(workflowId));
            if (job != null) {
                List<JobStep> jobSteps = job.getSteps();
                return jobSteps.stream().map(JobStep::getName).collect(Collectors.toList());
            } else {
                return null;
            }
        });

        ((WorkflowJobServiceImpl) workflowJobService).setWorkflowService(workflowService);
    }

    private void mockWorkflowContainerService() {
        WorkflowContainerService containerService = Mockito.mock(WorkflowContainerService.class);
        Mockito.when(containerService.getJobStatus("application_1549391605495_10000", null)).thenAnswer(invocation -> {
            com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus =
                    new com.latticeengines.domain.exposed.dataplatform.JobStatus();
            jobStatus.setStatus(FinalApplicationStatus.UNDEFINED);
            jobStatus.setState(YarnApplicationState.RUNNING);
            return jobStatus;
        });
        Mockito.when(containerService.getJobStatus("application_1549391605495_10001", null)).thenAnswer(invocation -> {
            com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus =
                    new com.latticeengines.domain.exposed.dataplatform.JobStatus();
            jobStatus.setStatus(FinalApplicationStatus.UNDEFINED);
            jobStatus.setState(YarnApplicationState.SUBMITTED);
            return jobStatus;
        });
        Mockito.when(containerService.getJobStatus("application_1549391605495_10002", null)).thenAnswer(invocation -> {
            com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus =
                    new com.latticeengines.domain.exposed.dataplatform.JobStatus();
            jobStatus.setStatus(FinalApplicationStatus.SUCCEEDED);
            jobStatus.setState(YarnApplicationState.FINISHED);
            return jobStatus;
        });
        Mockito.when(containerService.getJobStatus("application_1549391605495_10003", null)).thenAnswer(invocation -> {
            com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus =
                    new com.latticeengines.domain.exposed.dataplatform.JobStatus();
            jobStatus.setStatus(FinalApplicationStatus.FAILED);
            jobStatus.setState(YarnApplicationState.KILLED);
            return jobStatus;
        });
        Mockito.when(containerService.getJobStatus("application_1549391605495_20000", null)).thenAnswer(invocation -> {
            com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus =
                    new com.latticeengines.domain.exposed.dataplatform.JobStatus();
            jobStatus.setStatus(FinalApplicationStatus.KILLED);
            jobStatus.setState(YarnApplicationState.KILLED);
            return jobStatus;
        });
        Mockito.when(containerService.getJobStatus("application_1549391605495_30000", null)).thenAnswer(invocation -> {
            com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus =
                    new com.latticeengines.domain.exposed.dataplatform.JobStatus();
            jobStatus.setStatus(FinalApplicationStatus.UNDEFINED);
            jobStatus.setState(YarnApplicationState.SUBMITTED);
            return jobStatus;
        });
        Mockito.when(containerService.getJobStatus("application_1549391605495_30001", null)).thenAnswer(invocation -> {
            com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus =
                    new com.latticeengines.domain.exposed.dataplatform.JobStatus();
            jobStatus.setStatus(FinalApplicationStatus.UNDEFINED);
            jobStatus.setState(YarnApplicationState.RUNNING);
            return jobStatus;
        });

        ((WorkflowJobServiceImpl) workflowJobService).setWorkflowContainerService(containerService);
    }
}
