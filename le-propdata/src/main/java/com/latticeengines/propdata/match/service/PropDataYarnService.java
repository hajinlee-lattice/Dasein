package com.latticeengines.propdata.match.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;

public interface PropDataYarnService {

    ApplicationId submitPropDataJob(PropDataJobConfiguration jobConfiguration);

}
