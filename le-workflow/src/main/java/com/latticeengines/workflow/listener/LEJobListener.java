package com.latticeengines.workflow.listener;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import com.latticeengines.common.exposed.util.JsonUtils;

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

    public <V> V getObjectFromContext(JobExecution jobExecution, String key, Class<V> clazz) {
        String strValue = getStringValueFromContext(jobExecution, key);
        return JsonUtils.deserialize(strValue, clazz);
    }

    public <V> List<V> getListObjectFromContext(JobExecution jobExecution, String key, Class<V> clazz) {
        List<?> list = getObjectFromContext(jobExecution, key, List.class);
        return JsonUtils.convertList(list, clazz);
    }
}
