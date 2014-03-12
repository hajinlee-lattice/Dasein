package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.ThrottleConfigurationDao;
import com.latticeengines.dataplatform.entitymanager.SequenceEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;

@Component("throttleConfigurationEntityMgr")
public class ThrottleConfigurationEntityMgrImpl extends BaseEntityMgrImpl<ThrottleConfiguration> implements ThrottleConfigurationEntityMgr {

    @Autowired
    private ThrottleConfigurationDao throttleConfigurationDao;
    
    @Autowired
    private SequenceEntityMgr sequenceEntityMgr;
    
    public ThrottleConfigurationEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ThrottleConfiguration> getDao() {
        return throttleConfigurationDao;
    }

    @Override
    public void post(ThrottleConfiguration config) {
        if (config.getId() == null) {
            config.setId(sequenceEntityMgr.nextVal(ThrottleConfigurationDao.class));
        }
        super.post(config);
    }

}
