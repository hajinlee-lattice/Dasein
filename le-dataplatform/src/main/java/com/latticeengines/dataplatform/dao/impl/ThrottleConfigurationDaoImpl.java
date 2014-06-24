package com.latticeengines.dataplatform.dao.impl;

import com.latticeengines.dataplatform.dao.ThrottleConfigurationDao;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

public class ThrottleConfigurationDaoImpl extends BaseDaoImpl<ThrottleConfiguration> implements  ThrottleConfigurationDao {
    
    @Override
    protected Class<ThrottleConfiguration> getEntityClass() {
        return ThrottleConfiguration.class;
    }
    

}
