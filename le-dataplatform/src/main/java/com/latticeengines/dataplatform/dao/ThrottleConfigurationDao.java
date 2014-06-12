package com.latticeengines.dataplatform.dao;

import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

public interface ThrottleConfigurationDao extends BaseDao<ThrottleConfiguration> {
    void deleteAll();
}
