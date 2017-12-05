package com.latticeengines.workflowapi.service.impl;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.dao.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.mockito.Mockito;

import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.exposed.service.WorkflowTenantService;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowJobService;
import com.latticeengines.workflowapi.service.WorkflowContainerService;

public class WorkflowJobServiceImplTestNG extends WorkflowApiFunctionalTestNGBase {
    @Autowired
    TenantEntityMgr tenantEntityMgr;

    @Autowired
    private WorkflowJobService workflowJobService;

    @Autowired
    private WorkflowTenantService workflowTenantService;

    private Map<Long, Long> workflowIds = new HashMap<>();

    private JobInstanceDao jobInstanceDao;

    private JobExecutionDao jobExecutionDao;

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

    @Test(groups = "functional")
    public void testGetJobStatus() {
        createWorkflowJobs();

        JobStatus status = workflowJobService.getJobStatus(workflowIds.get(10000L));
        Assert.assertEquals(status, JobStatus.PENDING);

        status = workflowJobService.getJobStatus(workflowIds.get(10001L));
        Assert.assertEquals(status, JobStatus.PENDING);

        status = workflowJobService.getJobStatus(workflowIds.get(10002L));
        Assert.assertEquals(status, JobStatus.COMPLETED);

        status = workflowJobService.getJobStatus(workflowIds.get(10003L));
        Assert.assertEquals(status, JobStatus.FAILED);

        status = workflowJobService.getJobStatus(workflowIds.get(20000L));
        Assert.assertEquals(status, JobStatus.CANCELLED);

        List<Long> testWorkflowIds1 = new ArrayList<>();
        testWorkflowIds1.add(workflowIds.get(10000L));
        testWorkflowIds1.add(workflowIds.get(20000L));
        testWorkflowIds1.add(workflowIds.getOrDefault(90000L, 90000L));
        List<JobStatus> statuses = workflowJobService.getJobStatus(testWorkflowIds1);
        Assert.assertEquals(statuses.size(), 3);
        Assert.assertEquals(statuses.get(0), JobStatus.PENDING);
        Assert.assertEquals(statuses.get(1), JobStatus.CANCELLED);
        Assert.assertNull(statuses.get(2));

        List<Long> testWorkflowIds2 = new ArrayList<>();
        testWorkflowIds2.add(workflowIds.get(10000L));
        testWorkflowIds2.add(workflowIds.get(10002L));
        statuses = workflowJobService.getJobStatus(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds2);
        Assert.assertEquals(statuses.size(), 2);
        Assert.assertEquals(statuses.get(0), JobStatus.PENDING);
        Assert.assertEquals(statuses.get(1), JobStatus.COMPLETED);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetJobStatus")
    public void testGetJobsByTenant() {
        mockWorkflowContainerService();
        List<Job> jobs = workflowJobService.getJobsByTenant(tenant.getPid());
        Assert.assertEquals(jobs.size(), 5);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10000");
        Assert.assertEquals(jobs.get(0).getJobType(), "application_10000");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_10001");
        Assert.assertEquals(jobs.get(1).getJobType(), "application_10001");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_10002");
        Assert.assertEquals(jobs.get(2).getJobType(), "application_10002");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_10003");
        Assert.assertEquals(jobs.get(3).getJobType(), "application_10003");
        Assert.assertEquals(jobs.get(4).getApplicationId(), "application_20000");
        Assert.assertEquals(jobs.get(4).getJobType(), "application_20000");
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetJobStatus", "testGetJobsByTenant" })
    public void testGetJobs() {
        mockWorkflowService();
        List<Long> workflowExecutionIds = new ArrayList<>(workflowIds.values());
        List<Job> jobs = workflowJobService.getJobs(workflowExecutionIds);
        Assert.assertEquals(jobs.size(), 5);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10000");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_10001");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_10002");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_10003");
        Assert.assertEquals(jobs.get(4).getApplicationId(), "application_20000");

        jobs = workflowJobService.getJobs(workflowExecutionIds, "application_10002");
        Assert.assertEquals(jobs.size(), 1);
        Assert.assertEquals(jobs.get(0).getJobType(), "application_10002");
        jobs = workflowJobService.getJobs(workflowExecutionIds, "application_20000");
        Assert.assertEquals(jobs.size(), 1);
        Assert.assertEquals(jobs.get(0).getJobType(), "application_20000");

        jobs = workflowJobService.getJobs(StringUtils.EMPTY, null, null, false, false,0L);
        Assert.assertNull(jobs);
        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), null, null, false, false, 0L);
        Assert.assertEquals(jobs.size(), 5);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10000");
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_10001");
        Assert.assertEquals(jobs.get(2).getApplicationId(), "application_10002");
        Assert.assertEquals(jobs.get(3).getApplicationId(), "application_10003");
        Assert.assertEquals(jobs.get(4).getApplicationId(), "application_20000");

        Set<Long> testWorkflowIds1 = new HashSet<>();
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

        Set<String> testTypes = new HashSet<>();
        testTypes.add("application_10001");
        testTypes.add("application_10002");
        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1, testTypes, false, false, 0L);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10001");
        Assert.assertNull(jobs.get(0).getSteps());
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_10002");
        Assert.assertNull(jobs.get(1).getSteps());

        jobs = workflowJobService.getJobs(WFAPITEST_CUSTOMERSPACE.toString(), testWorkflowIds1, testTypes, true, false, 0L);
        Assert.assertEquals(jobs.size(), 2);
        Assert.assertEquals(jobs.get(0).getApplicationId(), "application_10001");
        Assert.assertNotNull(jobs.get(0).getSteps());
        Assert.assertEquals(jobs.get(1).getApplicationId(), "application_10002");
        Assert.assertNotNull(jobs.get(1).getSteps());

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
        WorkflowExecutionId executionId = new WorkflowExecutionId(workflowIds.get(10000L));
        List<String> stepNames = workflowJobService.getStepNames(executionId);
        Assert.assertNotNull(stepNames);
        Assert.assertEquals(stepNames.size(), 0);

        executionId = new WorkflowExecutionId(workflowIds.getOrDefault(90000L, 90000L));
        Assert.assertNull(workflowJobService.getStepNames(executionId));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetJobStatus", "testGetJobs" })
    public void testUpdateParentJobId() {
        List<Long> testWorkflowIds = new ArrayList<>();
        testWorkflowIds.add(workflowIds.get(10001L));
        testWorkflowIds.add(workflowIds.get(10002L));
        testWorkflowIds.add(workflowIds.get(10003L));
        testWorkflowIds.add(workflowIds.getOrDefault(90000L, 90000L));
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

    private void createWorkflowJobs() {
        jobInstanceDao = new MapJobInstanceDao();
        jobExecutionDao = new MapJobExecutionDao();

        Map<String, String> inputContext = new HashMap<>();
        inputContext.put(WorkflowContextConstants.Inputs.JOB_TYPE, "testJob");

        String applicationId = "application_10000";
        Map<String, JobParameter> parameters1 = new HashMap<>();
        parameters1.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters1 = new JobParameters(parameters1);
        JobInstance jobInstance1 = jobInstanceDao.createJobInstance(applicationId, jobParameters1);
        JobExecution jobExecution1 = new JobExecution(jobInstance1, jobParameters1, null);
        jobExecutionDao.saveJobExecution(jobExecution1);
        WorkflowJob workflowJob1 = new WorkflowJob();
        workflowJob1.setApplicationId(applicationId);
        workflowJob1.setInputContext(inputContext);
        workflowJob1.setTenant(tenant);
        workflowJob1.setStatus(JobStatus.fromYarnStatus(FinalApplicationStatus.UNDEFINED).name());
        workflowJob1.setWorkflowId(jobExecution1.getJobId());
        workflowJob1.setType(jobInstance1.getJobName());
        workflowJob1.setParentJobId(null);
        workflowJobEntityMgr.create(workflowJob1);
        workflowIds.put(10000L, jobExecution1.getJobId());

        applicationId = "application_10001";
        Map<String, JobParameter> parameters11 = new HashMap<>();
        parameters11.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters11 = new JobParameters(parameters11);
        JobInstance jobInstance11 = jobInstanceDao.createJobInstance(applicationId, jobParameters11);
        JobExecution jobExecution11 = new JobExecution(jobInstance11, jobParameters11, null);
        jobExecutionDao.saveJobExecution(jobExecution11);
        WorkflowJob workflowJob11 = new WorkflowJob();
        workflowJob11.setApplicationId(applicationId);
        workflowJob11.setInputContext(inputContext);
        workflowJob11.setTenant(tenant);
        workflowJob11.setStatus(JobStatus.fromYarnStatus(FinalApplicationStatus.UNDEFINED).name());
        workflowJob11.setWorkflowId(jobExecution11.getJobId());
        workflowJob11.setType(jobInstance11.getJobName());
        workflowJob11.setParentJobId(workflowIds.get(10000L));
        workflowJobEntityMgr.create(workflowJob11);
        workflowIds.put(10001L, jobExecution11.getJobId());

        applicationId = "application_10002";
        Map<String, JobParameter> parameters12 = new HashMap<>();
        parameters12.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters12 = new JobParameters(parameters12);
        JobInstance jobInstance12 = jobInstanceDao.createJobInstance(applicationId, jobParameters12);
        JobExecution jobExecution12 = new JobExecution(jobInstance12, jobParameters12, null);
        jobExecutionDao.saveJobExecution(jobExecution12);
        WorkflowJob workflowJob12 = new WorkflowJob();
        workflowJob12.setApplicationId(applicationId);
        workflowJob12.setInputContext(inputContext);
        workflowJob12.setTenant(tenant);
        workflowJob12.setStatus(JobStatus.fromYarnStatus(FinalApplicationStatus.SUCCEEDED).name());
        workflowJob12.setWorkflowId(jobExecution12.getJobId());
        workflowJob12.setType(jobInstance12.getJobName());
        workflowJob12.setParentJobId(workflowIds.get(10000L));
        workflowJobEntityMgr.create(workflowJob12);
        workflowIds.put(10002L, jobExecution12.getJobId());

        applicationId = "application_10003";
        Map<String, JobParameter> parameters13 = new HashMap<>();
        parameters13.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters13 = new JobParameters(parameters13);
        JobInstance jobInstance13 = jobInstanceDao.createJobInstance(applicationId, jobParameters13);
        JobExecution jobExecution13 = new JobExecution(jobInstance13, jobParameters13, null);
        jobExecutionDao.saveJobExecution(jobExecution13);
        WorkflowJob workflowJob13 = new WorkflowJob();
        workflowJob13.setApplicationId(applicationId);
        workflowJob13.setInputContext(inputContext);
        workflowJob13.setTenant(tenant);
        workflowJob13.setStatus(JobStatus.fromYarnStatus(FinalApplicationStatus.FAILED).name());
        workflowJob13.setWorkflowId(jobExecution13.getJobId());
        workflowJob13.setType(jobInstance13.getJobName());
        workflowJob13.setParentJobId(workflowIds.get(10000L));
        workflowJobEntityMgr.create(workflowJob13);
        workflowIds.put(10003L, jobExecution13.getJobId());

        applicationId = "application_20000";
        Map<String, JobParameter> parameters2 = new HashMap<>();
        parameters2.put("jobName", new JobParameter(applicationId, true));
        JobParameters jobParameters2 = new JobParameters(parameters2);
        JobInstance jobInstance2 = jobInstanceDao.createJobInstance(applicationId, jobParameters2);
        JobExecution jobExecution2 = new JobExecution(jobInstance2, jobParameters2, null);
        jobExecutionDao.saveJobExecution(jobExecution2);
        WorkflowJob workflowJob2 = new WorkflowJob();
        workflowJob2.setApplicationId(applicationId);
        workflowJob2.setInputContext(inputContext);
        workflowJob2.setTenant(tenant);
        workflowJob2.setStatus(JobStatus.fromYarnStatus(FinalApplicationStatus.KILLED).name());
        workflowJob2.setWorkflowId(jobExecution2.getJobId());
        workflowJob2.setType(jobInstance2.getJobName());
        workflowJobEntityMgr.create(workflowJob2);
        workflowIds.put(20000L, jobExecution2.getJobId());
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
                    List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByTenant(tenant);
                    return workflowJobs.stream().map(workflowJobToJobMapper).collect(Collectors.toList());
                }
        );
        ((WorkflowJobServiceImpl) workflowJobService).setWorkflowContainerService(workflowContainerService);
    }
}
