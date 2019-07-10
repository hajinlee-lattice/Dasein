package com.latticeengines.datacloud.etl.purge.entitymgr.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
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
    public List<PurgeStrategy> findAll() {
        return purgeStrategyDao.findAll();
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public List<PurgeStrategy> findStrategiesByType(SourceType sourceType) {
        return purgeStrategyDao.findAllByField("sourceType", sourceType);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public PurgeStrategy findStrategyBySource(String source) {
        List<PurgeStrategy> list = purgeStrategyDao.findAllByField("source", source);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        } else {
            return list.get(0);
        }
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public PurgeStrategy findStrategyBySourceAndType(String source, SourceType sourceType) {
        return purgeStrategyDao.findByFields("source", source, "sourceType", sourceType);
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
    public void delete(PurgeStrategy strategy) {
        purgeStrategyDao.delete(strategy);
    }

}
