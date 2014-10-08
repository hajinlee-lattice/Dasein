package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class CreateSamples extends ModelingResourceJob<SamplingConfiguration, List<String>> {

    public List<String> executeJob() throws Exception {
        return createSamples();
    }

    public List<String> createSamples() throws Exception {
        List<String> applicationIds = rc.createSamples(config);
        log.info(applicationIds);
        return applicationIds;
    }
}