package com.latticeengines.workflowapi.service.impl;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.mockito.Mockito;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.*;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.exposed.service.WorkflowTenantService;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowJobService;
import com.latticeengines.workflowapi.service.WorkflowContainerService;

import javax.annotation.PostConstruct;

public class WorkflowJobServiceImplTestNG extends WorkflowApiFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImplTestNG.class);

    @Autowired
    TenantEntityMgr tenantEntityMgr;

    @Autowired
    private WorkflowJobService workflowJobService;

    @Autowired
    private WorkflowTenantService workflowTenantService;

    private Map<Long, Long> workflowIds = new HashMap<>();

    private JobInstanceDao jobInstanceDao;

    private JobExecutionDao jobExecutionDao;

    private StepExecutionDao stepExecutionDao;

    private WorkflowJob workflowJob1;
    private WorkflowJob workflowJob11;
    private WorkflowJob workflowJob12;
    private WorkflowJob workflowJob13;
    private WorkflowJob workflowJob2;
    private WorkflowJob workflowJob3;
    private WorkflowJob workflowJob31;

    private WorkflowJobUpdate jobUpdate1;
    private WorkflowJobUpdate jobUpdate11;
    private WorkflowJobUpdate jobUpdate12;
    private WorkflowJobUpdate jobUpdate13;
    private WorkflowJobUpdate jobUpdate2;
    private WorkflowJobUpdate jobUpdate3;
    private WorkflowJobUpdate jobUpdate31;

    private long heartbeatThreshold;
    private long currentTime;
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

    @PostConstruct
    public void init() {
        heartbeatThreshold = TimeUnit.MILLISECONDS.convert(2L, TimeUnit.MINUTES);
        currentTime = System.currentTimeMillis();
        successTime = currentTime - heartbeatThreshold;
        failedTime = currentTime - 3 * heartbeatThreshold;
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

    @Test(groups = "functional")
    public void testGetJobStatus() {
        createWorkflowJobs();

        JobStatus status = workflowJobService.getJobStatus(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10000L));
        Assert.assertEquals(status, JobStatus.RUNNING);

        status = workflowJobService.getJobStatus(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10001L));
        Assert.assertEquals(status, JobStatus.FAILED);

        status = workflowJobService.getJobStatus(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10002L));
        Assert.assertEquals(status, JobStatus.COMPLETED);

        status = workflowJobService.getJobStatus(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(10003L));
        Assert.assertEquals(status, JobStatus.FAILED);

        status = workflowJobService.getJobStatus(WFAPITEST_CUSTOMERSPACE.toString(), workflowIds.get(20000L));
        Assert.assertEquals(status, JobStatus.FAILED);

        status = workflowJobService.getJobStatus(customerSpace3.toString(), workflowIds.get(30000L));
        Assert.assertEquals(status, JobStatus.PENDING);

        status = workflowJobService.getJobStatus(customerSpace3.toString(), workflowIds.get(30001L));
        Assert.assertEquals(status, JobStatus.FAILED);

        List<Long> testWorkflowIds1 = new ArrayList<>();
        testWorkflowIds1.add(workflowIds.get(10000L));
        testWorkflowIds1.add(workflowIds.get(20000L));
        testWorkflowIds1.add(workflowIds.getOrDefault(90000L, 90000L));
        List<JobStatus> statuses = workflowJobService.getJobStatus(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.RUNNING);
        Assert.assertEquals(statuses.get(1), JobStatus.FAILED);

        List<Long> testWorkflowIds2 = new ArrayList<>();
        testWorkflowIds2.add(workflowIds.get(10000L));
        testWorkflowIds2.add(workflowIds.get(10002L));
        statuses = workflowJobService.getJobStatus(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds2);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.RUNNING);
        Assert.assertEquals(statuses.get(1), JobStatus.COMPLETED);

        MultiTenantContext.setTenant(tenant);
        statuses = workflowJobService.getJobStatus(null, testWorkflowIds1);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.RUNNING);
        Assert.assertEquals(statuses.get(1), JobStatus.FAILED);

        statuses = workflowJobService.getJobStatus(null, testWorkflowIds2);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.RUNNING);
        Assert.assertEquals(statuses.get(1), JobStatus.COMPLETED);

        status = workflowJobService.getJobStatus(customerSpace3.toString(), workflowIds.get(30001L));
        Assert.assertNotNull(status);
        Assert.assertEquals(status, JobStatus.FAILED);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetJobStatus",
            expectedExceptions = { RuntimeException.class },
            expectedExceptionsMessageRegExp = "No tenant found with id .*")
    public void testGetJobs() {
        mockWorkflowService();
        setupLEJobExecutionRetriever();

        List<Job> jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), false);
        Assert.assertEquals(jobs.size(), 5);
        List<String> applicationIds = jobs.stream().map(Job::getApplicationId).collect(Collectors.toList());
        Assert.assertTrue(applicationIds.contains("application_10000"));
        Assert.assertTrue(applicationIds.contains("application_10001"));
        Assert.assertTrue(applicationIds.contains("application_10002"));
        Assert.assertTrue(applicationIds.contains("application_10003"));
        Assert.assertTrue(applicationIds.contains("application_20000"));

        jobs = workflowJobService.getJobs(customerSpace3.toString(), false);
        Assert.assertEquals(jobs.size(), 2);
        applicationIds = jobs.stream().map(Job::getApplicationId).collect(Collectors.toList());
        Assert.assertTrue(applicationIds.contains("application_30000"));
        Assert.assertTrue(applicationIds.contains("application_30001"));

        MultiTenantContext.setTenant(null);
        jobs = workflowJobService.getJobs(null, false);
        Assert.assertEquals(jobs.size(), 7);
        applicationIds = jobs.stream().map(Job::getApplicationId).collect(Collectors.toList());
        Assert.assertTrue(applicationIds.contains("application_10000"));
        Assert.assertTrue(applicationIds.contains("application_10001"));
        Assert.assertTrue(applicationIds.contains("application_10002"));
        Assert.assertTrue(applicationIds.contains("application_10003"));
        Assert.assertTrue(applicationIds.contains("application_20000"));
        Assert.assertTrue(applicationIds.contains("application_30000"));
        Assert.assertTrue(applicationIds.contains("application_30001"));

        List<Long> workflowExecutionIds = new ArrayList<>(workflowIds.values());
        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), workflowExecutionIds, null, true, null, null);
        Assert.assertEquals(jobs.size(), 5);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10000");
        Assert.assertEquals(jobs.get(0).getSteps().size(), 2);
        Assert.assertEquals(jobs.get(0).getSteps().get(0).getJobStepType(), "step1_2");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_10001");
        Assert.assertEquals(jobs.get(1).getSteps().size(), 3);
        Assert.assertEquals(jobs.get(1).getSteps().get(1).getJobStepType(), "step11_2");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_10002");
        Assert.assertEquals(jobs.get(2).getSteps().size(), 1);
        Assert.assertEquals(jobs.get(2).getSteps().get(0).getJobStepType(), "step12_1");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_10003");
        Assert.assertEquals(jobs.get(3).getSteps().size(), 3);
        Assert.assertEquals(jobs.get(3).getSteps().get(2).getJobStepType(), "step13_1");
        Assert.assertEquals(jobs.get(4).getApplicationId(), "application_20000");
        Assert.assertEquals(jobs.get(4).getSteps().size(), 9);
        Assert.assertEquals(jobs.get(4).getSteps().get(1).getJobStepType(), "step2_8");

        List<String> workflowExecutionTypes = new ArrayList<>();
        workflowExecutionTypes.add("application_30000");
        workflowExecutionTypes.add("application_30001");
        jobs = workflowJobService.getJobs(customerSpace3.toString(), null, workflowExecutionTypes, true, null, null);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_30000");
        Assert.assertEquals(jobs.get(0).getSteps().size(), 1);
        Assert.assertEquals(jobs.get(0).getSteps().get(0).getJobStepType(), "step3_1");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_30001");
        Assert.assertEquals(jobs.get(1).getSteps().size(), 4);
        Assert.assertEquals(jobs.get(1).getSteps().get(3).getJobStepType(), "step31_1");

        MultiTenantContext.setTenant(tenant3);
        workflowExecutionTypes.add("application_10000");
        jobs = workflowJobService.getJobs(null, null, workflowExecutionTypes, true, null, null);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_30000");
        Assert.assertEquals(jobs.get(0).getSteps().size(), 1);
        Assert.assertEquals(jobs.get(0).getSteps().get(0).getJobStepType(), "step3_1");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_30001");
        Assert.assertEquals(jobs.get(1).getSteps().size(), 4);
        Assert.assertEquals(jobs.get(1).getSteps().get(3).getJobStepType(), "step31_1");

        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), workflowExecutionIds,
                Collections.singletonList("application_10002"), true, false, -1L);
        Assert.assertEquals(jobs.size(), 1);
        Assert.assertEquals(jobs.get(0).getJobType(), "application_10002");
        Assert.assertEquals(jobs.get(0).getSteps().size(), 1);
        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), workflowExecutionIds,
                Collections.singletonList("application_20000"), false, false, -1L);
        Assert.assertEquals(jobs.size(), 1);
        Assert.assertEquals(jobs.get(0).getJobType(), "application_20000");
        Assert.assertNull(jobs.get(0).getSteps());

        workflowJobService.getJobs(StringUtils.EMPTY, null, null, false, false, 0L);
        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), null, null, false, false, 0L);
        Assert.assertEquals(jobs.size(), 5);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10000");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_10001");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_10002");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_10003");
        Assert.assertEquals(jobs.get(4).getApplicationId(), "application_20000");

        List<Long> testWorkflowIds1 = new ArrayList<>();
        testWorkflowIds1.add(workflowIds.get(10000L));
        testWorkflowIds1.add(workflowIds.get(10001L));
        testWorkflowIds1.add(workflowIds.get(10002L));
        testWorkflowIds1.add(workflowIds.get(20000L));
        testWorkflowIds1.add(workflowIds.getOrDefault(90000L, 90000L));
        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1, null, false, false, 0L);
        Assert.assertEquals(jobs.size(), 4);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10000");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_10001");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_10002");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_20000");

        List<String> testTypes = new ArrayList<>();
        testTypes.add("application_10001");
        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1, testTypes, false, false, 0L);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10001");
        Assert.assertNull(jobs.get(0).getSteps());

        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1, testTypes, true, false, 0L);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10001");
        Assert.assertNotNull(jobs.get(0).getSteps());

        Long parentJobId = workflowIds.get(10000L);
        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1, null, true, true, parentJobId);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10001");
        Assert.assertNotNull(jobs.get(0).getSteps());
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_10002");
        Assert.assertNotNull(jobs.get(1).getSteps());
    }

    @Test(groups = "functional", dependsOnMethods = "testGetJobStatus")
    public void testGetStepNames() {
        mockWorkflowService();
        setupLEJobExecutionRetriever();
        Long workflowId = workflowIds.get(10000L);
        List<String> stepNames = workflowJobService.getStepNames(WFAPITEST_CUSTOMERSPACE.toString(), workflowId);
        Assert.assertNotNull(stepNames);
        Assert.assertEquals(stepNames.size(), 2);
        Assert.assertEquals(stepNames.get(0), "step1_2");
        Assert.assertEquals(stepNames.get(1), "step1_1");

        workflowId = workflowIds.getOrDefault(90000L, 90000L);
        Assert.assertNull(workflowJobService.getStepNames(WFAPITEST_CUSTOMERSPACE.toString(), workflowId));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetJobStatus", "testGetJobs" })
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
        workflowJobService.updateParentJobId(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds, parentJobId);
        workflowjob0 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(0));
        workflowjob1 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(1));
        workflowjob2 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds.get(2));
        Assert.assertEquals(workflowjob0.getParentJobId(), parentJobId);
        Assert.assertEquals(workflowjob1.getParentJobId(), parentJobId);
        Assert.assertEquals(workflowjob2.getParentJobId(), parentJobId);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetJobStatus")
    public void testMultiTenantFilter() {
        List<Long> testWorkflowIds1 = new ArrayList<>();
        testWorkflowIds1.add(workflowIds.get(30000L));
        testWorkflowIds1.add(workflowIds.get(30001L));
        List<JobStatus> status1 = workflowJobService.getJobStatus(customerSpace3.toString(), testWorkflowIds1);
        Assert.assertEquals(status1.size(), 2);
        Assert.assertEquals(status1.get(0), JobStatus.PENDING);
        Assert.assertEquals(status1.get(1), JobStatus.FAILED);

        List<Long> testWorkflowIds2 = new ArrayList<>();
        testWorkflowIds2.add(workflowIds.get(10000L));
        testWorkflowIds2.add(workflowIds.get(10002L));
        testWorkflowIds2.add(workflowIds.get(30001L));
        List<JobStatus> status2 = workflowJobService.getJobStatus(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds2);
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

        workflowJobService.updateParentJobId(customerSpace3.toString(), testWorkflowIds2, parentJobId);
        workflowJob0 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds2.get(0));
        workflowJob1 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds2.get(1));
        workflowJob2 = workflowJobEntityMgr.findByWorkflowId(testWorkflowIds2.get(2));
        Assert.assertNull(workflowJob0);
        Assert.assertNull(workflowJob1);
        Assert.assertEquals(workflowJob2.getParentJobId(), parentJobId);
    }

    private void createWorkflowJobs() {
        jobInstanceDao = new MapJobInstanceDao();
        jobExecutionDao = new MapJobExecutionDao();
        stepExecutionDao = new MapStepExecutionDao();

        Map<String, String> inputContext = new HashMap<>();
        inputContext.put(WorkflowContextConstants.Inputs.JOB_TYPE, "testJob");

        String applicationId = "application_10000";
        Map<String, JobParameter> parameters1 = new HashMap<>();
        parameters1.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters1 = new JobParameters(parameters1);
        JobInstance jobInstance1 = jobInstanceDao.createJobInstance(applicationId, jobParameters1);
        JobExecution jobExecution1 = new JobExecution(jobInstance1, jobParameters1, null);
        jobExecutionDao.saveJobExecution(jobExecution1);
        List<StepExecution> steps = new ArrayList<>();
        steps.add(new StepExecution("step1_1", jobExecution1));
        steps.add(new StepExecution("step1_2", jobExecution1));
        stepExecutionDao.saveStepExecutions(steps);
        workflowJob1 = new WorkflowJob();
        workflowJob1.setApplicationId(applicationId);
        workflowJob1.setInputContext(inputContext);
        workflowJob1.setTenant(tenant);
        workflowJob1.setStatus(JobStatus.fromString(FinalApplicationStatus.UNDEFINED.name(), YarnApplicationState.RUNNING).name());
        workflowJob1.setWorkflowId(jobExecution1.getJobId());
        workflowJob1.setType(jobInstance1.getJobName());
        workflowJob1.setParentJobId(null);
        workflowJobEntityMgr.create(workflowJob1);
        workflowIds.put(10000L, jobExecution1.getJobId());
        jobUpdate1 = new WorkflowJobUpdate();
        jobUpdate1.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob1.getWorkflowId()).getPid());
        jobUpdate1.setLastUpdateTime(successTime);
        workflowJobUpdateEntityMgr.create(jobUpdate1);

        applicationId = "application_10001";
        Map<String, JobParameter> parameters11 = new HashMap<>();
        parameters11.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters11 = new JobParameters(parameters11);
        JobInstance jobInstance11 = jobInstanceDao.createJobInstance(applicationId, jobParameters11);
        JobExecution jobExecution11 = new JobExecution(jobInstance11, jobParameters11, null);
        jobExecutionDao.saveJobExecution(jobExecution11);
        steps.clear();
        steps.add(new StepExecution("step11_1", jobExecution11));
        steps.add(new StepExecution("step11_2", jobExecution11));
        steps.add(new StepExecution("step11_3", jobExecution11));
        stepExecutionDao.saveStepExecutions(steps);
        workflowJob11 = new WorkflowJob();
        workflowJob11.setApplicationId(applicationId);
        workflowJob11.setInputContext(inputContext);
        workflowJob11.setTenant(tenant);
        workflowJob11.setStatus(JobStatus.fromString(FinalApplicationStatus.UNDEFINED.name()).name());
        workflowJob11.setWorkflowId(jobExecution11.getJobId());
        workflowJob11.setType(jobInstance11.getJobName());
        workflowJob11.setParentJobId(workflowIds.get(10000L));
        workflowJobEntityMgr.create(workflowJob11);
        workflowIds.put(10001L, jobExecution11.getJobId());
        jobUpdate11 = new WorkflowJobUpdate();
        jobUpdate11.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob11.getWorkflowId()).getPid());
        jobUpdate11.setLastUpdateTime(failedTime);
        workflowJobUpdateEntityMgr.create(jobUpdate11);

        applicationId = "application_10002";
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
        workflowJob12.setInputContext(inputContext);
        workflowJob12.setTenant(tenant);
        workflowJob12.setStatus(JobStatus.fromString(FinalApplicationStatus.SUCCEEDED.name()).name());
        workflowJob12.setWorkflowId(jobExecution12.getJobId());
        workflowJob12.setType(jobInstance12.getJobName());
        workflowJob12.setParentJobId(workflowIds.get(10000L));
        workflowJobEntityMgr.create(workflowJob12);
        workflowIds.put(10002L, jobExecution12.getJobId());
        jobUpdate12 = new WorkflowJobUpdate();
        jobUpdate12.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob12.getWorkflowId()).getPid());
        jobUpdate12.setLastUpdateTime(failedTime);
        workflowJobUpdateEntityMgr.create(jobUpdate12);

        applicationId = "application_10003";
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
        workflowJob13.setInputContext(inputContext);
        workflowJob13.setTenant(tenant);
        workflowJob13.setStatus(JobStatus.fromString(FinalApplicationStatus.FAILED.name()).name());
        workflowJob13.setWorkflowId(jobExecution13.getJobId());
        workflowJob13.setType(jobInstance13.getJobName());
        workflowJob13.setParentJobId(workflowIds.get(10000L));
        workflowJobEntityMgr.create(workflowJob13);
        workflowIds.put(10003L, jobExecution13.getJobId());
        jobUpdate13 = new WorkflowJobUpdate();
        jobUpdate13.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob13.getWorkflowId()).getPid());
        jobUpdate13.setLastUpdateTime(failedTime);
        workflowJobUpdateEntityMgr.create(jobUpdate13);

        applicationId = "application_20000";
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
        workflowJob2.setInputContext(inputContext);
        workflowJob2.setTenant(tenant);
        workflowJob2.setStatus(JobStatus.fromString(FinalApplicationStatus.KILLED.name()).name());
        workflowJob2.setWorkflowId(jobExecution2.getJobId());
        workflowJob2.setType(jobInstance2.getJobName());
        workflowJobEntityMgr.create(workflowJob2);
        workflowIds.put(20000L, jobExecution2.getJobId());
        jobUpdate2 = new WorkflowJobUpdate();
        jobUpdate2.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob2.getWorkflowId()).getPid());
        jobUpdate2.setLastUpdateTime(successTime);
        workflowJobUpdateEntityMgr.create(jobUpdate2);

        createTenant();
        applicationId = "application_30000";
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
        workflowJob3.setInputContext(inputContext);
        workflowJob3.setTenant(tenant3);
        workflowJob3.setStatus(JobStatus.fromString(FinalApplicationStatus.UNDEFINED.name()).name());
        workflowJob3.setWorkflowId(jobExecution3.getJobId());
        workflowJob3.setType(jobInstance3.getJobName());
        workflowJobEntityMgr.create(workflowJob3);
        workflowIds.put(30000L, jobExecution3.getJobId());
        jobUpdate3 = new WorkflowJobUpdate();
        jobUpdate3.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob3.getWorkflowId()).getPid());
        jobUpdate3.setLastUpdateTime(successTime);
        workflowJobUpdateEntityMgr.create(jobUpdate3);

        applicationId = "application_30001";
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
        workflowJob31.setInputContext(inputContext);
        workflowJob31.setTenant(tenant3);
        workflowJob31.setStatus(JobStatus.fromString(FinalApplicationStatus.UNDEFINED.name(), YarnApplicationState.RUNNING).name());
        workflowJob31.setWorkflowId(jobExecution31.getJobId());
        workflowJob31.setType(jobInstance31.getJobName());
        workflowJobEntityMgr.create(workflowJob31);
        workflowIds.put(30001L, jobExecution31.getJobId());
        jobUpdate31 = new WorkflowJobUpdate();
        jobUpdate31.setWorkflowPid(workflowJobEntityMgr.findByWorkflowId(workflowJob31.getWorkflowId()).getPid());
        jobUpdate31.setLastUpdateTime(failedTime);
        workflowJobUpdateEntityMgr.create(jobUpdate31);
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
        ((WorkflowJobServiceImpl) workflowJobService).setLeJobExecutionRetriever(leJobExecutionRetriever);
    }

    @SuppressWarnings("unchecked")
    private void mockWorkflowService() {
        WorkflowService workflowService = Mockito.mock(WorkflowService.class);
        Mockito.when(workflowService.getJobs(Mockito.anyList())).thenAnswer(
                invocation -> {
                    List<WorkflowExecutionId> executionIds = (List<WorkflowExecutionId>) invocation.getArguments()[0];
                    List<Long> workflowIds = executionIds.stream().map(WorkflowExecutionId::getId)
                            .collect(Collectors.toList());
                    List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIds(workflowIds);
                    return workflowJobs.stream().map(workflowJobToJobMapper).collect(Collectors.toList());
                }
        );
        Mockito.when(workflowService.getJobs(Mockito.anyList(), Mockito.anyString())).thenAnswer(
                invocation -> {
                    List<WorkflowExecutionId> executionIds = (List<WorkflowExecutionId>) invocation.getArguments()[0];
                    String type = (String) invocation.getArguments()[1];
                    List<Long> workflowIds = executionIds.stream().map(WorkflowExecutionId::getId)
                            .collect(Collectors.toList());
                    List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIds(workflowIds);
                    workflowJobs.removeIf(workflowJob -> !workflowJob.getType().equals(type));
                    return workflowJobs.stream().map(workflowJobToJobMapper).collect(Collectors.toList());
                }
        );
        Mockito.when(workflowService.getStepNames(Mockito.any(WorkflowExecutionId.class))).thenAnswer(
                invocation -> {
                    Long workflowId = ((WorkflowExecutionId) invocation.getArguments()[0]).getId();
                    Job job = workflowJobToJobMapper.apply(workflowJobEntityMgr.findByWorkflowId(workflowId));
                    if (job != null) {
                        List<JobStep> jobSteps = job.getSteps();
                        return jobSteps.stream().map(JobStep::getName).collect(Collectors.toList());
                    } else {
                        return null;
                    }
                }
        );
        ((WorkflowJobServiceImpl) workflowJobService).setWorkflowService(workflowService);
    }

    private void mockWorkflowContainerService() {
        WorkflowContainerService workflowContainerService = Mockito.mock(WorkflowContainerService.class);
        Mockito.when(workflowContainerService.getJobsByTenant(Mockito.anyLong())).thenAnswer(
                invocation -> {
                    Long tenantPid = (Long) invocation.getArguments()[0];
                    Tenant tenant = workflowTenantService.getTenantByTenantPid(tenantPid);
                    MultiTenantContext.setTenant(tenant);
                    List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findAll();
                    return workflowJobs.stream().map(workflowJobToJobMapper).collect(Collectors.toList());
                }
        );
        ((WorkflowJobServiceImpl) workflowJobService).setWorkflowContainerService(workflowContainerService);
    }
}
