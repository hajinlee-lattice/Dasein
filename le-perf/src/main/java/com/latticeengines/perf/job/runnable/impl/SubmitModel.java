package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public abstract class SubmitModel extends ModelingResourceJob<Model, List<String>> {

    public SubmitModel() {
    }

    public SubmitModel(String customer, String restEndpointHost) {
        super(customer, restEndpointHost);
    }

    public List<String> executeJob(Model model) throws Exception {
        return submitModel(model);
    }

    public List<String> submitModel(Model model) throws Exception {
        List<String> applicationIds = rc.submitModel(model);
        log.info(applicationIds);
        return applicationIds;
    }
}
