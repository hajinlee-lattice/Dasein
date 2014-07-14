package com.latticeengines.perf.job.runnable.impl;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class GetJobStatus extends ModelingResourceJob<String, JobStatus> {

    @Override
    public JobStatus executeJob() throws Exception {
        return getJobStatus();
    }

    public JobStatus getJobStatus() throws Exception {
        JobStatus js = rc.getJobStatus(config);
        log.info("Application " + js.getId() + " is in progress " + js.getProgress());
        return js;
    }
}
