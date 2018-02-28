package com.latticeengines.datacloud.etl.purge.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.etl.purge.dao.PurgeStrategyDao;
import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component("purgeStrategyEntityMgr")
public class PurgeStrategyEntityMgrImpl implements PurgeStrategyEntityMgr {

    @Autowired
    private PurgeStrategyDao purgeStrategyDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public List<PurgeStrategy> findStrategiesByType(SourceType sourceType) {
        return purgeStrategyDao.findAllByField("sourceType", sourceType);
    }

    @Override
    @VisibleForTesting
    @Transactional(value = "propDataManage")
    public void insertAll(List<PurgeStrategy> strategies) {
        strategies.forEach(strategy -> {
            purgeStrategyDao.create(strategy);
        });
    }

    @Override
    @VisibleForTesting
    @Transactional(value = "propDataManage")
    public void deleteAll() {
        purgeStrategyDao.deleteAll();
    }
}
