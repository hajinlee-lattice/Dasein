package com.latticeengines.dataplatform.dao.impl;

import org.springframework.stereotype.Repository;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.dao.ThrottleConfigurationDao;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@Repository("throttleConfigurationDao")
public class ThrottleConfigurationDaoImpl extends BaseDaoImpl<ThrottleConfiguration> implements
        ThrottleConfigurationDao {

    @Override
    protected Class<ThrottleConfiguration> getEntityClass() {
        return ThrottleConfiguration.class;
    }

    @Override
    public ThrottleConfiguration deserialize(String id, String content) {
        return JsonUtils.deserialize(content, ThrottleConfiguration.class);
    }

    @Override
    public String serialize(ThrottleConfiguration config) {
        return config.toString();
    }

}
