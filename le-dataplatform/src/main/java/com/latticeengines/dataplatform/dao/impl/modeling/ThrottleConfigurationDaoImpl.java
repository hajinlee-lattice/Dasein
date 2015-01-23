package com.latticeengines.dataplatform.dao.impl.modeling;

import com.latticeengines.dataplatform.dao.modeling.ThrottleConfigurationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;

public class ThrottleConfigurationDaoImpl extends BaseDaoImpl<ThrottleConfiguration> implements  ThrottleConfigurationDao {
    
    @Override
    protected Class<ThrottleConfiguration> getEntityClass() {
        return ThrottleConfiguration.class;
    }
    

}
