package com.latticeengines.workflow.listener;

import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;

import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;

public class FailureReportingListener extends LEJobListener {
    private static final Logger log = LoggerFactory.getLogger(FailureReportingListener.class);

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
                for (Throwable throwable : exceptions) {
                    if (throwable instanceof LedpException) {
                        log.info("Job execution %s failed with error details %s", jobExecution.getId(),
                                ((LedpException) throwable).getErrorDetails());
                    }
                }
                if (exceptions.size() > 0) {
                    Throwable exception = exceptions.get(0);

                    ErrorDetails details;
                    if (exception instanceof LedpException) {
                        LedpException casted = (LedpException) exception;
                        details = casted.getErrorDetails();
                    } else {
                        details = new ErrorDetails(LedpCode.LEDP_00002, exception.getMessage(),
                                ExceptionUtils.getStackTrace(exception));
                    }
                    job.setErrorDetails(details);
                    workflowJobEntityMgr.updateErrorDetails(job);
                } else {
                    job.setErrorDetails(new ErrorDetails(LedpCode.LEDP_00002, LedpCode.LEDP_00002.getMessage(), null));
                }
            } else {
                log.warn(String.format("Could not find workflow job with id %s", jobExecution.getId()));
            }
        }
    }
}
