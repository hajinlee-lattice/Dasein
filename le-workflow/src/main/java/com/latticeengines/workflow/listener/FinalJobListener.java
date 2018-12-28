package com.latticeengines.workflow.listener;

import com.latticeengines.workflow.exposed.service.JobCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;

import java.util.Collections;

@Component("finalJobListener")
public class FinalJobListener extends LEJobListener implements LEJobCallerRegister {

    private static final Logger log = LoggerFactory.getLogger(FinalJobListener.class);

    private volatile LEJobCaller caller;
    private volatile Thread callerThread;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private JobCacheService jobCacheService;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        Long executionId = jobExecution.getId();
        try {
            if (!updateStatus(executionId, jobExecution)) {
                throw new RuntimeException("Can not update workflow job status, Id=" + executionId);
            }

            clearJobCache(executionId);
        } finally {
            if (caller != null) {
                log.info("Workflow finished (workflowId={})", executionId);
                caller.callDone();
                callerThread.interrupt();
            } else {
                log.error("Should not have a null LEJobCaller, cannot finish the job properly");
            }
        }
    }

    private void clearJobCache(Long workflowId) {
        if (workflowId == null) {
            return;
        }

        jobCacheService.evictByWorkflowIds(Collections.singletonList(workflowId));
    }

    private boolean updateStatus(Long executionId, JobExecution jobExecution) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(executionId);
        if (workflowJob == null) {
            log.warn("There's no workflow job found, Id=" + executionId);
            return false;
        }
        WorkflowStatus status = workflowService.getStatus(new WorkflowExecutionId(executionId), jobExecution);
        log.info("Job status=" + jobExecution.getStatus() + " workflow Id=" + jobExecution.getId());
        workflowJob.setStatus(JobStatus.fromString(status.getStatus().name()).name());
        workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
        log.info("Updated work flow status=" + status + " workflow Id=" + executionId);
        return true;
    }

    @Override
    public void register(Thread callerThread, LEJobCaller caller) {
        log.info("Thread {} register LEJobCaller {}", callerThread, caller);
        this.callerThread = callerThread;
        this.caller = caller;
    }
}
