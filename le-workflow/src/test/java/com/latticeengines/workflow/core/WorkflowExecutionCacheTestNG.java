package com.latticeengines.workflow.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@Component
public class WorkflowExecutionCacheTestNG extends WorkflowTestNGBase {

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private WorkflowExecutionCache cache;

    @Value("${workflow.jobs.expireTime:60}")
    private String expireTime;

    @Test(groups = "functional")
    public void testStaticCache() {
        Long workflowId = 100L;
        setupStaticJob(100L, BatchStatus.COMPLETED, FinalApplicationStatus.SUCCEEDED);
        Job job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.COMPLETED);
        assertEquals(cache.staticCacheSize(), 1);
        assertEquals(cache.dynamicCacheSize(), 0);

        workflowId = 101L;
        setupStaticJob(workflowId,BatchStatus.FAILED, FinalApplicationStatus.FAILED);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.FAILED);
        assertEquals(cache.staticCacheSize(), 2);
        assertEquals(cache.dynamicCacheSize(), 0);

        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.FAILED);
        assertEquals(cache.staticCacheSize(), 2);
        assertEquals(cache.dynamicCacheSize(), 0);

        workflowId = 102L;
        setupStaticJob(workflowId, BatchStatus.ABANDONED, FinalApplicationStatus.FAILED);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.FAILED);
        assertEquals(cache.staticCacheSize(), 3);
        assertEquals(cache.dynamicCacheSize(), 0);

        workflowId = 103L;
        setupStaticJob(workflowId, BatchStatus.STOPPED, FinalApplicationStatus.UNDEFINED);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.CANCELLED);
        assertEquals(cache.staticCacheSize(), 4);
        assertEquals(cache.dynamicCacheSize(), 0);

        workflowId = 100L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.COMPLETED);
        assertEquals(cache.staticCacheSize(), 4);
        assertEquals(cache.dynamicCacheSize(), 0);

        workflowId = 102L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.FAILED);
        assertEquals(cache.staticCacheSize(), 4);
        assertEquals(cache.dynamicCacheSize(), 0);

        workflowId = 104L;
        setupStaticJob(workflowId, BatchStatus.STOPPING, FinalApplicationStatus.UNDEFINED);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.CANCELLED);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 0);

        workflowId = 103L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.CANCELLED);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 0);

        workflowId = 104L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.CANCELLED);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 0);
    }

    @Test(groups = "functional", dependsOnMethods = "testStaticCache")
    public void testDynamicCache() {
        Long workflowId = 0L;
        setupDynamicJob(workflowId, BatchStatus.STARTING, FinalApplicationStatus.UNDEFINED,
                YarnApplicationState.ACCEPTED, true);
        Job job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.PENDING);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 1);

        workflowId = 1L;
        setupDynamicJob(workflowId, BatchStatus.STARTED, FinalApplicationStatus.UNDEFINED,
                YarnApplicationState.RUNNING, true);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.RUNNING);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 2);

        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.RUNNING);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 2);

        workflowId = 2L;
        setupDynamicJob(workflowId, BatchStatus.STARTED, FinalApplicationStatus.UNDEFINED,
                YarnApplicationState.RUNNING, true);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.RUNNING);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 3);

        workflowId = 3L;
        setupDynamicJob(workflowId, BatchStatus.STARTED, FinalApplicationStatus.UNDEFINED,
                YarnApplicationState.ACCEPTED, true);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.PENDING);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 4);

        workflowId = 0L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.PENDING);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 4);

        workflowId = 2L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.RUNNING);
        assertEquals(cache.staticCacheSize(), 5);
        assertEquals(cache.dynamicCacheSize(), 4);
    }

    @Test(groups = "functional", dependsOnMethods = { "testStaticCache", "testDynamicCache" })
    public void testStaticDynamicCache() {
        Long workflowId = 300L;
        setupStaticJob(workflowId, BatchStatus.COMPLETED, FinalApplicationStatus.SUCCEEDED);
        Job job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.COMPLETED);
        assertEquals(cache.staticCacheSize(), 6);
        assertEquals(cache.dynamicCacheSize(), 4);

        workflowId = 301L;
        setupStaticJob(workflowId, BatchStatus.FAILED, FinalApplicationStatus.FAILED);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.FAILED);
        assertEquals(cache.staticCacheSize(), 7);
        assertEquals(cache.dynamicCacheSize(), 4);

        workflowId = 200L;
        setupDynamicJob(workflowId, BatchStatus.STARTING, FinalApplicationStatus.UNDEFINED,
                YarnApplicationState.ACCEPTED, true);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.PENDING);
        assertEquals(cache.staticCacheSize(), 7);
        assertEquals(cache.dynamicCacheSize(), 5);

        workflowId = 300L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.COMPLETED);
        assertEquals(cache.staticCacheSize(), 7);
        assertEquals(cache.dynamicCacheSize(), 5);

        workflowId = 200L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.PENDING);
        assertEquals(cache.staticCacheSize(), 7);
        assertEquals(cache.dynamicCacheSize(), 5);

        workflowId = 302L;
        setupStaticJob(workflowId, BatchStatus.STOPPED, FinalApplicationStatus.UNDEFINED);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.CANCELLED);
        assertEquals(cache.staticCacheSize(), 8);
        assertEquals(cache.dynamicCacheSize(), 5);

        workflowId = 201L;
        setupDynamicJob(workflowId, BatchStatus.STARTED, FinalApplicationStatus.UNDEFINED,
                YarnApplicationState.RUNNING, true);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.RUNNING);
        assertEquals(cache.staticCacheSize(), 8);
        assertEquals(cache.dynamicCacheSize(), 6);

        workflowId = 302L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.CANCELLED);
        assertEquals(cache.staticCacheSize(), 8);
        assertEquals(cache.dynamicCacheSize(), 6);

        workflowId = 201L;
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), workflowId);
        assertEquals(job.getJobStatus(), JobStatus.RUNNING);
        assertEquals(cache.staticCacheSize(), 8);
        assertEquals(cache.dynamicCacheSize(), 6);
    }

    @Test(groups = "functional", dependsOnMethods = {
            "testStaticCache", "testDynamicCache", "testStaticDynamicCache"})
    public void testDynamicJobCanExpire() {
        Long workflowId = 400L;
        setupStaticJob(workflowId, BatchStatus.STOPPED, FinalApplicationStatus.UNDEFINED);
        Job job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), new Long(400L));
        assertEquals(job.getJobStatus(), JobStatus.CANCELLED);
        assertEquals(cache.staticCacheSize(), 9);
        assertEquals(cache.dynamicCacheSize(), 6);

        workflowId = 500L;
        setupDynamicJob(workflowId, BatchStatus.STARTING, FinalApplicationStatus.UNDEFINED,
                YarnApplicationState.ACCEPTED, true);
        job = cache.getJob(new WorkflowExecutionId(workflowId));
        assertEquals(job.getId(), new Long(500L));
        assertEquals(job.getJobStatus(), JobStatus.PENDING);
        assertEquals(cache.staticCacheSize(), 9);
        assertEquals(cache.dynamicCacheSize(), 7);

        try {
            Thread.sleep(5 * 1000);
            cache.cleanUp();
            assertEquals(cache.staticCacheSize(), 9);
            assertEquals(cache.dynamicCacheSize(), 7);

            setupDynamicJob(workflowId, BatchStatus.STARTED, FinalApplicationStatus.UNDEFINED,
                    YarnApplicationState.RUNNING, false);
            job = cache.getJob(new WorkflowExecutionId(workflowId));
            assertEquals(job.getId(), new Long(500L));
            assertEquals(job.getJobStatus(), JobStatus.PENDING);  // status in cache is still 'PENDING'
            assertEquals(cache.staticCacheSize(), 9);
            assertEquals(cache.dynamicCacheSize(), 7);

            Thread.sleep(5 * 1000);
            cache.cleanUp();
            assertEquals(cache.staticCacheSize(), 9);
            assertEquals(cache.dynamicCacheSize(), 7);

            Thread.sleep(Long.parseLong(expireTime) * 1000);
            cache.cleanUp();
            assertEquals(cache.staticCacheSize(), 9);
            assertEquals(cache.dynamicCacheSize(), 0);

            job = cache.getJob(new WorkflowExecutionId(workflowId));
            assertEquals(job.getId(), new Long(500L));
            assertEquals(job.getJobStatus(), JobStatus.RUNNING);  // latest status in cache is 'RUNNING'
            assertEquals(cache.staticCacheSize(), 9);
            assertEquals(cache.dynamicCacheSize(), 1);

            Thread.sleep(5 * 1000);
            setupDynamicJob(workflowId, BatchStatus.COMPLETED, FinalApplicationStatus.SUCCEEDED,
                    YarnApplicationState.FINISHED, false);
            job = cache.getJob(new WorkflowExecutionId(workflowId));
            assertEquals(job.getId(), new Long(500L));
            assertEquals(job.getJobStatus(), JobStatus.RUNNING);  // status in cache is still 'RUNNING'
            assertEquals(cache.staticCacheSize(), 9);
            assertEquals(cache.dynamicCacheSize(), 1);

            Thread.sleep(Long.parseLong(expireTime) * 1000);
            setupDynamicJob(workflowId, BatchStatus.COMPLETED, FinalApplicationStatus.SUCCEEDED,
                    YarnApplicationState.FINISHED, false);
            job = cache.getJob(new WorkflowExecutionId(workflowId));
            assertEquals(job.getId(), new Long(500L));
            assertEquals(job.getJobStatus(), JobStatus.COMPLETED);  // status in cache is 'COMPLETED'
            assertEquals(cache.staticCacheSize(), 10);
            assertEquals(cache.dynamicCacheSize(), 0);
        } catch (Exception exc) {
            Assert.fail("Testing thread is interrupted. Test failed.");
        }
    }

    private WorkflowJob createWorkflowJobWithFinalStatus(Long workflowId, FinalApplicationStatus status) {
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setWorkflowId(workflowId);
        workflowJob.setTenant(MultiTenantContext.getTenant());
        workflowJob.setApplicationId("applicationid_" + workflowId);
        workflowJob.setStatus(status);
        return workflowJob;
    }

    private WorkflowService mockWorkflowServiceWithBatchStatus(BatchStatus status) {
        WorkflowService workflowService = mock(WorkflowService.class);
        when(workflowService.getStatus(any(WorkflowExecutionId.class))).thenReturn(new WorkflowStatus(status));
        return workflowService;
    }

    private LEJobExecutionRetriever mockLeJobExecutionRetriever(JobExecution execution) {
        LEJobExecutionRetriever leJobExecutionRetriever = mock(LEJobExecutionRetriever.class);
        when(leJobExecutionRetriever.getJobExecution(anyLong())).thenReturn(execution);
        return leJobExecutionRetriever;
    }

    private JobProxy mockJobProxyWithJobStatus(com.latticeengines.domain.exposed.dataplatform.JobStatus status) {
        JobProxy proxy = mock(JobProxy.class);
        when(proxy.getJobStatus(anyString())).thenReturn(status);
        return proxy;
    }

    private void setupStaticJob(Long workflowId, BatchStatus batchStatus, FinalApplicationStatus appStatus) {
        WorkflowJob workflowJob = createWorkflowJobWithFinalStatus(workflowId, appStatus);
        workflowJobEntityMgr.create(workflowJob);
        WorkflowService workflowService = mockWorkflowServiceWithBatchStatus(batchStatus);
        cache.setWorkflowService(workflowService);
        JobExecution execution = new JobExecution(workflowId);
        execution.setStatus(batchStatus);
        execution.setJobInstance(new JobInstance(workflowId, "name_" + workflowId));
        LEJobExecutionRetriever leJobExecutionRetriever = mockLeJobExecutionRetriever(execution);
        cache.setLEJobExecutionRetriever(leJobExecutionRetriever);
    }

    private void setupDynamicJob(Long workflowId, BatchStatus batchStatus,
                                 FinalApplicationStatus finalApplicationStatus,
                                 YarnApplicationState yarnState, boolean isCreate) {
        WorkflowJob workflowJob = createWorkflowJobWithFinalStatus(workflowId, finalApplicationStatus);
        if (isCreate) {
            workflowJobEntityMgr.create(workflowJob);
        }
        WorkflowService workflowService = mockWorkflowServiceWithBatchStatus(batchStatus);
        cache.setWorkflowService(workflowService);
        com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus = new
                com.latticeengines.domain.exposed.dataplatform.JobStatus();
        jobStatus.setStatus(finalApplicationStatus);
        jobStatus.setState(yarnState);
        JobProxy proxy = mockJobProxyWithJobStatus(jobStatus);
        cache.setJobProxy(proxy);
        JobExecution execution = new JobExecution(workflowId);
        execution.setStatus(batchStatus);
        execution.setJobInstance(new JobInstance(workflowId, "name_" + workflowId));
        LEJobExecutionRetriever leJobExecutionRetriever = mockLeJobExecutionRetriever(execution);
        cache.setLEJobExecutionRetriever(leJobExecutionRetriever);
    }
}
