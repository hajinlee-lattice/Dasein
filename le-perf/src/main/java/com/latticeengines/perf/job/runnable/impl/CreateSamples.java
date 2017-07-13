package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class CreateSamples extends ModelingResourceJob<SamplingConfiguration, List<String>> {

    public List<String> executeJob() throws Exception {
        return createSamples();
    }

    public List<String> createSamples() throws Exception {
        List<String> applicationIds = rc.createSamples(config);
        log.info(StringUtils.join(", ", applicationIds));
        return applicationIds;
    }
}