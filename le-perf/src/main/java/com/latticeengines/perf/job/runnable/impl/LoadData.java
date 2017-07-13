package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class LoadData extends ModelingResourceJob<LoadConfiguration, List<String>> {

    public List<String> executeJob() throws Exception {
        return loadData();
    }

    public List<String> loadData() throws Exception {
        List<String> applicationIds = rc.loadData(config);
        log.info(StringUtils.join(", ", applicationIds));
        return applicationIds;
    }

}
