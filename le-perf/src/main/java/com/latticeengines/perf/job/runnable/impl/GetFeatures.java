package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class GetFeatures extends ModelingResourceJob<Model, List<String>> {

    @Override
    public List<String> executeJob() throws Exception {
        return getFeatures();
    }

    public List<String> getFeatures() throws Exception {
        List<String> features = rc.getFeatures(config);
        log.info(StringUtils.join(", ", features));
        return features;
    }
}
