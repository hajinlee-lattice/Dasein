package com.latticeengines.metadata.entitymgr.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.metadata.dao.StatisticsContainerDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("statisticsContainerEntityMgr")
public class StatisticsContainerEntityMgrImpl extends BaseEntityMgrImpl<StatisticsContainer> implements
        StatisticsContainerEntityMgr {

    @Autowired
    private StatisticsContainerDao statisticsContainerDao;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

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
        if (existing == null) {
            createStatistics(container);
            return container;
        } else {
            existing.setStatistics(container.getStatistics());
            update(existing);
            return existing;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public StatisticsContainer createStatistics(StatisticsContainer container) {
        container.setTenant(MultiTenantContext.getTenant());
        if (StringUtils.isBlank(container.getName())) {
            container.setName(NamingUtils.timestamp("Stats"));
        }
        create(container);
        return container;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public StatisticsContainer findInSegment(String segmentName) {
        return statisticsContainerDao.findInSegment(segmentName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public StatisticsContainer findInMasterSegment(String collectionName) {
        collectionName = StringUtils.isBlank(collectionName) ? dataCollectionEntityMgr.getDefaultCollectionName() : collectionName;
        return statisticsContainerDao.findInMasterSegment(collectionName);
    }
}
