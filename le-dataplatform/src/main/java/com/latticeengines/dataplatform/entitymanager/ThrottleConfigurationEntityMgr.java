package com.latticeengines.dataplatform.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

public interface ThrottleConfigurationEntityMgr extends BaseEntityMgr<ThrottleConfiguration> {

    List<ThrottleConfiguration> getConfigsSortedBySubmissionTime();

    ThrottleConfiguration getLatestConfig();

}
