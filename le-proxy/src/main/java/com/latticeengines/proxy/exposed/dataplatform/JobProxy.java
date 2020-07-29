package com.latticeengines.proxy.exposed.dataplatform;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("jobProxy")
public class JobProxy extends MicroserviceRestApiProxy {

    public JobProxy() {
        super("modeling");
    }

    public JobStatus getJobStatus(String applicationId) {
        String url = constructUrl("/jobs/{applicationId}", applicationId);
        return get("getJobStatus", url, JobStatus.class);
    }

    public Counters getMRJobCounters(String applicationId) {
        String url = constructUrl("/jobs/{applicationId}/counters", applicationId);
        return get("getMRJobCounters", url, Counters.class);
    }
}
