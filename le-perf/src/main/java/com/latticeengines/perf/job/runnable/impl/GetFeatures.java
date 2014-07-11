package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public abstract class GetFeatures extends ModelingResourceJob<Model, List<String>> {

    public GetFeatures() {
    }

    public GetFeatures(String customer, String restEndpointHost) {
        super(customer, restEndpointHost);
    }

    @Override
    public List<String> executeJob(Model model) throws Exception {
        return getFeatures(model);
    }

    public List<String> getFeatures(Model model) throws Exception {
        List<String> applicationIds = rc.getFeatures(model);
        log.info(applicationIds);
        return applicationIds;
    }
}
