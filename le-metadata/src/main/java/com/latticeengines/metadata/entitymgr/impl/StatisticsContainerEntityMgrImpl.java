package com.latticeengines.metadata.entitymgr.impl;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.metadata.dao.StatisticsContainerDao;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("statisticsContainerEntityMgr")
public class StatisticsContainerEntityMgrImpl extends BaseEntityMgrImpl<StatisticsContainer> implements
        StatisticsContainerEntityMgr {

    @Autowired
    private StatisticsContainerDao statisticsContainerDao;

    @Override
    public BaseDao<StatisticsContainer> getDao() {
        return statisticsContainerDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public StatisticsContainer findStatisticsByName(String statisticsName) {
        return statisticsContainerDao.findByField("name", statisticsName);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public StatisticsContainer createOrUpdateStatistics(StatisticsContainer container) {
        StatisticsContainer existing = findStatisticsByName(container.getName());
        if (existing != null) {
            delete(existing);
        } else {
            container.setName("Statistics_" + UUID.randomUUID());
        }
        container.setTenant(MultiTenantContext.getTenant());
        create(container);
        return container;
    }
}
