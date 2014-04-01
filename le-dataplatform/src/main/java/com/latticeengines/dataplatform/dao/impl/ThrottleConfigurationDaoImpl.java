package com.latticeengines.dataplatform.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.ThrottleConfigurationDao;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
import com.latticeengines.dataplatform.util.JsonHelper;

@Component("throttleConfigurationDao")
public class ThrottleConfigurationDaoImpl extends BaseDaoImpl<ThrottleConfiguration> implements ThrottleConfigurationDao {

    @Override
    public ThrottleConfiguration deserialize(String id, String content) {
        return JsonHelper.deserialize(content, ThrottleConfiguration.class);
    }

    @Override
    public String serialize(ThrottleConfiguration config) {
        return config.toString();
    }

}
