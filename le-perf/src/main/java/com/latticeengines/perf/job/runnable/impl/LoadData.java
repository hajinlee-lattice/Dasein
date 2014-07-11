package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public abstract class LoadData extends ModelingResourceJob<LoadConfiguration, List<String>> {

    public LoadData() {
    }

    public LoadData(String customer, String restEndpointHost) {
        super(customer, restEndpointHost);
    }

    public List<String> executeJob(LoadConfiguration config) throws Exception {
        return loadData(config);
    }

    public List<String> loadData(LoadConfiguration config) throws Exception {
        List<String> applicationIds = rc.loadData(config);
        log.info(applicationIds);
        return applicationIds;
    }
}
