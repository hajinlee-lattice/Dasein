package com.latticeengines.network.exposed.matchapi;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;

public interface MatchInternalInterface {

    AppSubmission submitYarnJob(DataCloudJobConfiguration dataCloudJobConfiguration);

}
