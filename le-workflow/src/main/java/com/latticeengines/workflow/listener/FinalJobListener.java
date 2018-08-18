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
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;

import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Component("finalJobListener")
public class FinalJobListener extends LEJobListener implements LEJobCallerRegister {

    private static final long UPDATE_CACHE_TIMEOUT_IN_SECONDS = 15L;

    private static final Logger log = LoggerFactory.getLogger(FinalJobListener.class);

    private LEJobCaller caller;
    private Thread callerThread;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

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

            updateJobCache(executionId);
        } finally {
            if (caller != null) {
                caller.callDone();
                callerThread.interrupt();
            }
        }
    }

    private void updateJobCache(Long workflowId) {
        if (workflowId == null) {
            return;
        }

        jobCacheService.evictByWorkflowIds(Collections.singletonList(workflowId));
        Future<?> refreshFuture = jobCacheService.putAsync(workflowId);
        try {
            // wait for the cache to be refreshed
            refreshFuture.get(UPDATE_CACHE_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            // do nothing
        }
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
    public void regisger(Thread callerThread, LEJobCaller caller) {
        this.callerThread = callerThread;
        this.caller = caller;
    }

}
