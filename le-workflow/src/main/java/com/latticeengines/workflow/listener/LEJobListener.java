package com.latticeengines.workflow.listener;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;

public abstract class LEJobListener implements JobExecutionListener {
    private static final Logger log = LoggerFactory.getLogger(LEJobListener.class);

    public abstract void beforeJobExecution(JobExecution jobExecution);

    public abstract void afterJobExecution(JobExecution jobExecution);

    @Override
    public final void beforeJob(JobExecution jobExecution) {
        try {
            beforeJobExecution(jobExecution);
        } catch (Exception e) {
            log.error(String.format("Caught error in job listener %s: %s", getClass().getName(), e.getMessage()), e);
        }
    }

    @Override
    public final void afterJob(JobExecution jobExecution) {
        try {
            afterJobExecution(jobExecution);
        } catch (Exception e) {
            log.error(String.format("Caught error in job listener %s: %s", getClass().getName(), e.getMessage()), e);
        }
    }

    public String getStringValueFromContext(JobExecution jobExecution, String key) {
        try {
            return jobExecution.getExecutionContext().getString(key);
        } catch (ClassCastException e) {
            return null;
        }
    }

    public <V> Set<V> getSetFromContext(JobExecution jobExecution, String key, Class<V> clazz) {
        Set<?> set = getObjectFromContext(jobExecution, key, Set.class);
        return JsonUtils.convertSet(set, clazz);
    }

    public <V> V getObjectFromContext(JobExecution jobExecution, String key, Class<V> clazz) {
        String strValue = getStringValueFromContext(jobExecution, key);
        return JsonUtils.deserialize(strValue, clazz);
    }

    public <V> List<V> getListObjectFromContext(JobExecution jobExecution, String key, Class<V> clazz) {
        List<?> list = getObjectFromContext(jobExecution, key, List.class);
        return JsonUtils.convertList(list, clazz);
    }

    /*-
     * evaluate whether this job can be retried (only used by PA atm)
     */
    protected boolean canRetry(@NotNull JobExecution jobExecution, DataFeed feed, int retryLimit) {
        if (jobExecution.getStatus() != BatchStatus.FAILED) {
            log.info("Job status {} is not failed, no need to retry", jobExecution.getStatus());
            return false;
        }

        boolean userError = isUserError(jobExecution);
        boolean reachRetryLimit = feed != null && feed.getActiveExecution() != null
                && feed.getActiveExecution().getRetryCount() >= retryLimit;

        boolean canRetry = !userError && !reachRetryLimit;

        log.info("CanRetry = {}, IsUserError = {}, ReachRetryLimit = {}, RetryLimit = {}", canRetry, userError,
                reachRetryLimit, retryLimit);
        return canRetry;
    }

    // jobExecution.status == BatchStatus.FAILED
    protected boolean isUserError(JobExecution jobExecution) {
        List<Throwable> exceptions = jobExecution.getAllFailureExceptions();

        ErrorDetails details;
        if (exceptions.size() > 0) {
            Throwable exception = exceptions.get(0);

            if (exception instanceof LedpException) {
                LedpException casted = (LedpException) exception;
                details = casted.getErrorDetails();
            } else {
                details = new ErrorDetails(LedpCode.LEDP_00002, exception.getMessage(),
                        ExceptionUtils.getStackTrace(exception));
            }
        } else {
            details = new ErrorDetails(LedpCode.LEDP_00002, LedpCode.LEDP_00002.getMessage(), null);
        }
        String errorCategory = WorkflowJobUtils.searchErrorCategory(details);
        boolean isUserError = WorkflowJobUtils.isUserError(errorCategory);
        log.info("Error detail for job {} = {}, error category = {}, isUserError = {}", jobExecution.getId(),
                JsonUtils.serialize(details), errorCategory, isUserError);
        return isUserError;
    }
}
