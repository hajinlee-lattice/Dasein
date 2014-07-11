package com.latticeengines.perf.job.runnable;

import java.util.concurrent.Callable;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.latticeengines.perf.rest.client.LedpRestClient;

public abstract class ModelingResourceJob<T, E> implements Callable<E> {

    protected String customer;
    protected LedpRestClient rc;
    protected static final Log log = LogFactory.getLog(ModelingResourceJob.class);

    public ModelingResourceJob() {
    }

    public ModelingResourceJob(String customer, String restEndpointHost) {
        this.customer = customer;
        this.rc = new LedpRestClient(restEndpointHost);
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public void setLedpRestClient(String restEndpointHost) {
        this.rc = new LedpRestClient(restEndpointHost);
    }

    public abstract T setConfiguration() throws Exception;

    public abstract E executeJob(T config) throws Exception;

    @Override
    public E call() throws Exception {
        try {
            T config = setConfiguration();
            return executeJob(config);
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
        }
        return null;
    }
}
