package com.latticeengines.workflow.listener;

import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;

import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;

public class FailureReportingListener extends LEJobListener {
    private static final Logger log = Logger.getLogger(FailureReportingListener.class);

    private WorkflowJobEntityMgr workflowJobEntityMgr;

    public FailureReportingListener(WorkflowJobEntityMgr workflowJobEntityMgr) {
        this.workflowJobEntityMgr = workflowJobEntityMgr;
    }

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.FAILED) {
            WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
            if (job != null) {
                List<Throwable> exceptions = jobExecution.getAllFailureExceptions();

                if (exceptions.size() > 0) {
                    Throwable exception = exceptions.get(0);

                    ErrorDetails details;
                    if (exception instanceof LedpException) {
                        LedpException casted = (LedpException) exception;
                        details = casted.getErrorDetails();
                    } else {
                        details = new ErrorDetails(LedpCode.LEDP_00002, exception.getMessage(),
                                ExceptionUtils.getFullStackTrace(exception));
                    }
                    job.setErrorDetails(details);
                    workflowJobEntityMgr.update(job);
                } else {
                    job.setErrorDetails(new ErrorDetails(LedpCode.LEDP_00002, LedpCode.LEDP_00002.getMessage(), null));
                }
            } else {
                log.warn(String.format("Could not find workflow job with id %s", jobExecution.getId()));
            }
        }
    }
}
