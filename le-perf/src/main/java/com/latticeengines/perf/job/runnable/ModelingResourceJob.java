package com.latticeengines.perf.job.runnable;

import java.util.concurrent.Callable;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.perf.rest.client.LedpRestClient;

public abstract class ModelingResourceJob<T, E> implements Callable<E> {

    protected String restEndpointHost;
    protected LedpRestClient rc;
    protected static final Log log = LogFactory.getLog(ModelingResourceJob.class);
    protected T config;

    public void setConfiguration(String restEndpointHost, T config) throws Exception {
        this.config = config;
        this.restEndpointHost = restEndpointHost;
        this.rc = new LedpRestClient(restEndpointHost);
    }

    public String getRestEndpointHost() {
        return this.restEndpointHost;
    }

    public abstract E executeJob() throws Exception;

    @Override
    public E call() {
        try {
            return executeJob();
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        return null;
    }
}
