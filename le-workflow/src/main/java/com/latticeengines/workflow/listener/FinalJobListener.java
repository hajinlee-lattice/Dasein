package com.latticeengines.workflow.listener;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.workflow.exposed.service.WorkflowService;

@Component("finalJobListener")
public class FinalJobListener extends LEJobListener implements LEJobCallerRegister {

    private static final Logger log = LoggerFactory.getLogger(FinalJobListener.class);

    private volatile LEJobCaller caller;
    private volatile Thread callerThread;
    private volatile boolean waitForCaller;

    @Inject
    private WorkflowService workflowService;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private JobCacheService jobCacheService;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
        Long executionId = jobExecution.getId();
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(executionId);
        WorkflowConfiguration workflowConfig = workflowJob.getWorkflowConfiguration();
        if (Boolean.TRUE.equals(workflowConfig.getEnableTempTable())) {
            metadataProxy.setEnableTempTables(true);
        }
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
            if (waitForCaller) {
                // NOTE this method is executed by Spring so there is a chance main thread has
                // not set caller yet, wait for it to finish
                try {
                    synchronized (this) {
                        while (caller == null) {
                            log.info("Waiting for LEJobCaller to be set");
                            wait(TimeUnit.SECONDS.toMillis(3L));
                        }
                    }
                } catch (Exception e) {
                    log.error("Error occurs when waiting for LEJobCaller to be set", e);
                }
            }

            if (caller != null) {
                log.info("Workflow finished (workflowId={})", executionId);
                caller.callDone();
                callerThread.interrupt();
            } else {
                log.warn("Got NULL LEJobCaller, workflow might not be able to finish properly");
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
        if (!BatchStatus.FAILED.equals(jobExecution.getStatus())) {
            Set<String> registeredTableNames = getObjectFromContext(jobExecution, BaseWorkflowStep.REGISTERED_TABLE_NAMES, Set.class);
            if (CollectionUtils.isNotEmpty(registeredTableNames)) {
                String customerSpace = workflowJob.getTenant().getId();
                try {
                    metadataProxy.keepTablesForever(customerSpace, registeredTableNames.stream().collect(Collectors.toList()));
                } catch (Exception e) {
                    log.error(String.format("Failed to update table retention policy when workflow %d finished.", workflowJob.getPid()), e);
                }

            }
        }
        return true;
    }

    @Override
    public void register(Thread callerThread, LEJobCaller caller) {
        log.info("Thread {} register LEJobCaller {}", callerThread, caller);
        this.callerThread = callerThread;
        this.caller = caller;
    }

    @Override
    public void enableWaitForCaller() {
        log.info("Listener will wait for LEJobCaller to be set");
        waitForCaller = true;
    }
}
