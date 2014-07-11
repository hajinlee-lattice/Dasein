package com.latticeengines.perf.job.runnable.impl;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class GetJobStatus extends ModelingResourceJob<String, JobStatus> {

    private String appId;

    public GetJobStatus() {
    }

    public GetJobStatus(String appId, String restEndpointHost) {
        this.appId = appId;
        setLedpRestClient(restEndpointHost);
    }

    @Override
    public String setConfiguration() {
        return this.appId;
    }

    @Override
    public JobStatus executeJob(String config) throws Exception {
        return getJobStatus(config);
    }

    public JobStatus getJobStatus(String appId) throws Exception {
        JobStatus js = rc.getJobStatus(appId);
        log.info("Application " + js.getId() + " is in progress " + js.getProgress());
        return js;
    }

}
