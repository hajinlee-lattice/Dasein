package com.latticeengines.workflow.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.beans.factory.annotation.Autowired;
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

public class WorkflowExecutionCacheTestNG extends WorkflowTestNGBase {

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private WorkflowExecutionCache workflowExecutionCache;

    @Test(groups = "functional", enabled = true)
    public void getJobStatusForJobCantFindInYarnWithSuccessfulStatus() {

        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setWorkflowId(1L);
        workflowJob.setTenant(MultiTenantContext.getTenant());
        workflowJob.setApplicationId("applicationid_0002");
        workflowJob.setStatus(FinalApplicationStatus.UNDEFINED);
        workflowJobEntityMgr.create(workflowJob);

        WorkflowService workflowService = mock(WorkflowService.class);
        when(workflowService.getStatus(any(JobExecution.class)))
                .thenReturn(new WorkflowStatus(BatchStatus.COMPLETED));

        workflowExecutionCache.setWorkflowService(workflowService);

        LEJobExecutionRetriever leJobExecutionRetriever = mock(LEJobExecutionRetriever.class);
        JobExecution execution = new JobExecution(1L);
        execution.setStatus(BatchStatus.COMPLETED);
        execution.setJobInstance(new JobInstance(1L, "name"));
        when(leJobExecutionRetriever.getJobExecution((any(Long.class)))).thenReturn(execution);

        workflowExecutionCache.setLEJobExecutionRetriever(leJobExecutionRetriever);

        Job job = workflowExecutionCache.getJob(new WorkflowExecutionId(1L));

        assertEquals(job.getJobStatus(), JobStatus.COMPLETED);
    }
}
