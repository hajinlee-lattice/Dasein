package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.ThrottleConfigurationDao;
import com.latticeengines.dataplatform.entitymanager.SequenceEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@Component("throttleConfigurationEntityMgr")
public class ThrottleConfigurationEntityMgrImpl extends BaseEntityMgrImpl<ThrottleConfiguration> implements ThrottleConfigurationEntityMgr {

    @Autowired
    private ThrottleConfigurationDao throttleConfigurationDao;
    
   ///     @Autowired
    private SequenceEntityMgr sequenceEntityMgr;
    
    public ThrottleConfigurationEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ThrottleConfiguration> getDao() {
        return throttleConfigurationDao;
    }

    //@Override
  /*  public void post(ThrottleConfiguration config) {
        if (config.getPid() == null) {
            config.setPid(sequenceEntityMgr.nextVal(ThrottleConfigurationDao.class));
        }
      //  super.post(config);
    }*/
       
    
    
    @Override
    public List<ThrottleConfiguration> getConfigsSortedBySubmissionTime() {
        List<ThrottleConfiguration> configs = throttleConfigurationDao.findAll();  ///getAll();
        
        Collections.sort(configs, new Comparator<ThrottleConfiguration>() {

            @Override
            public int compare(ThrottleConfiguration o1, ThrottleConfiguration o2) {
                // TODO Auto-generated method stub
                return 0;
            }
            
        });
        return configs;
    }

    @Override
    public ThrottleConfiguration getLatestConfig() {
        List<ThrottleConfiguration> configs = getConfigsSortedBySubmissionTime();
        if (configs.size() == 0) {
            return null;
        }
        return configs.get(0);
    }
    

}
