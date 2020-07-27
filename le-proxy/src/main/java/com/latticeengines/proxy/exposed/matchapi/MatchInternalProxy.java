package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class MatchInternalProxy extends BaseRestApiProxy {

    public MatchInternalProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/internal");
    }

    public AppSubmission submitYarnJob(DataCloudJobConfiguration jobConfiguration) {
        String url = constructUrl("/yarnjobs");
        return post("submitYarnJob", url, jobConfiguration, AppSubmission.class);
    }
}
