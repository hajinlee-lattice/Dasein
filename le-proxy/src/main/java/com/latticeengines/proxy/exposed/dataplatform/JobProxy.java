package com.latticeengines.proxy.exposed.dataplatform;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.network.exposed.dataplatform.JobInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("jobProxy")
public class JobProxy extends MicroserviceRestApiProxy implements JobInterface {

    public JobProxy() {
        super("modeling");
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        String url = constructUrl("/jobs/{applicationId}", applicationId);
        return get("getJobStatus", url, JobStatus.class);
    }

    @Override
    public Counters getMRJobCounters(String applicationId) {
        String url = constructUrl("/jobs/{applicationId}/counters", applicationId);
        return get("getMRJobCounters", url, Counters.class);
    }
}
