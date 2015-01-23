package com.latticeengines.dataplatform.entitymanager.modeling;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;

public interface ThrottleConfigurationEntityMgr extends BaseEntityMgr<ThrottleConfiguration> {

    List<ThrottleConfiguration> getConfigsSortedBySubmissionTime();

    ThrottleConfiguration getLatestConfig();
    
    void cleanUpAllConfiguration();

}
