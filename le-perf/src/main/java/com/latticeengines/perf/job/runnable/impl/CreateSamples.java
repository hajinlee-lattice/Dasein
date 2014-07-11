package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public abstract class CreateSamples extends ModelingResourceJob<SamplingConfiguration, List<String>> {

    public CreateSamples() {
    }

    public CreateSamples(String customer, String restEndpointHost) {
        super(customer, restEndpointHost);
    }

    public List<String> executeJob(SamplingConfiguration config) throws Exception {
        return createSamples(config);
    }

    public List<String> createSamples(SamplingConfiguration config) throws Exception {
        List<String> applicationIds = rc.createSamples(config);
        log.info(applicationIds);
        return applicationIds;
    }
}