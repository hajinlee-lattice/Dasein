package com.latticeengines.dataplatform.entitymanager;

import java.util.List;

import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;

public interface ThrottleConfigurationEntityMgr extends BaseEntityMgr<ThrottleConfiguration> {

    List<ThrottleConfiguration> getConfigsSortedBySubmissionTime();

    ThrottleConfiguration getLatestConfig();

}
