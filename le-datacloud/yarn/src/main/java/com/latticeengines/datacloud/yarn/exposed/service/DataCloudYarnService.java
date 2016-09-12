package com.latticeengines.datacloud.yarn.exposed.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;

public interface DataCloudYarnService {

    ApplicationId submitPropDataJob(DataCloudJobConfiguration jobConfiguration);

}
