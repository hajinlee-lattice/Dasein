package com.latticeengines.perf.job.runnable.impl;

import java.util.List;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class SubmitModel extends ModelingResourceJob<Model, List<String>> {

    public List<String> executeJob() throws Exception {
        return submitModel();
    }

    public List<String> submitModel() throws Exception {
        List<String> applicationIds = rc.submitModel(config);
        log.info(applicationIds);
        return applicationIds;
    }

    public Model getModel() {
        return config;
    }
}
