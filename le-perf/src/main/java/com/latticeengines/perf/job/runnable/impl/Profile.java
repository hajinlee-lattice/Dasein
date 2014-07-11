package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public abstract class Profile extends ModelingResourceJob<DataProfileConfiguration, List<String>> {

    public Profile() {
    }

    public Profile(String customer, String restEndpointHost) {
        super(customer, restEndpointHost);
    }

    public List<String> executeJob(DataProfileConfiguration config) throws Exception {
        return profile(config);
    }

    public List<String> profile(DataProfileConfiguration config) throws Exception {
        List<String> applicationIds = rc.profile(config);
        log.info(applicationIds);
        return applicationIds;
    }
}
