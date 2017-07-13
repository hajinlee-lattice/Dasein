package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;
import org.apache.commons.lang3.StringUtils;

public class SubmitModel extends ModelingResourceJob<Model, List<String>> {

    public List<String> executeJob() throws Exception {
        return submitModel();
    }

    public List<String> submitModel() throws Exception {
        GetFeatures gf = new GetFeatures();
        gf.setConfiguration(restEndpointHost, config);
        List<String> featureList = gf.getFeatures();
        config.setFeaturesList(featureList);

        List<String> applicationIds = rc.submitModel(config);
        log.info(StringUtils.join(", ", applicationIds));
        return applicationIds;
    }

    public Model getModel() {
        return config;
    }
}
