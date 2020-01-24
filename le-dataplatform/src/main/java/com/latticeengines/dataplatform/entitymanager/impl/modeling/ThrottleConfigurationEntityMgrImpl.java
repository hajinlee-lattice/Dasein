package com.latticeengines.dataplatform.entitymanager.impl.modeling;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.modeling.ThrottleConfigurationDao;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;

@Component("throttleConfigurationEntityMgr")
public class ThrottleConfigurationEntityMgrImpl extends BaseEntityMgrImpl<ThrottleConfiguration> implements
        ThrottleConfigurationEntityMgr {

    @Inject
    private ThrottleConfigurationDao throttleConfigurationDao;

    public ThrottleConfigurationEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ThrottleConfiguration> getDao() {
        return throttleConfigurationDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<ThrottleConfiguration> getConfigsSortedBySubmissionTime() {
        List<ThrottleConfiguration> configs = throttleConfigurationDao.findAll();

        Collections.sort(configs, new Comparator<ThrottleConfiguration>() {

            @Override
            public int compare(ThrottleConfiguration o1, ThrottleConfiguration o2) {
                boolean smaller = o1.getTimestamp().before(o2.getTimestamp());

                return (smaller ? 1 : -1);
            }
        });
        return configs;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ThrottleConfiguration getLatestConfig() {
        List<ThrottleConfiguration> configs = getConfigsSortedBySubmissionTime();
        if (configs.size() == 0) {
            return null;
        }
        return configs.get(0);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void cleanUpAllConfiguration() {
        throttleConfigurationDao.deleteAll();
    }

}
