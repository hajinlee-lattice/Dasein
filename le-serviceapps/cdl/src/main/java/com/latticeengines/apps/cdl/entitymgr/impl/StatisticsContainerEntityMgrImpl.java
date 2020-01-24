package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.StatisticsContainerDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;

@SuppressWarnings("deprecation")
@Component("statisticsContainerEntityMgr")
public class StatisticsContainerEntityMgrImpl extends BaseEntityMgrImpl<StatisticsContainer>
        implements StatisticsContainerEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(StatisticsContainerEntityMgrImpl.class);

    @Inject
    private StatisticsContainerDao statisticsContainerDao;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Override
    public BaseDao<StatisticsContainer> getDao() {
        return statisticsContainerDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public StatisticsContainer findStatisticsByName(String statisticsName) {
        return statisticsContainerDao.findByField("name", statisticsName);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public StatisticsContainer createOrUpdateStatistics(StatisticsContainer container) {
        StatisticsContainer existing = findStatisticsByName(container.getName());
        if (existing == null) {
            createStatistics(container);
            return container;
        } else {
            existing.setStatistics(container.getStatistics());
            existing.setStatsCubes(container.getStatsCubes());
            update(existing);
            return existing;
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
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
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public StatisticsContainer findInSegment(String segmentName, DataCollection.Version version) {
        StatisticsContainer container = statisticsContainerDao.findInSegment(segmentName, version);
        return copyStatisticsToStatsCubes(container);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public StatisticsContainer findInMasterSegment(String collectionName, DataCollection.Version version) {
        collectionName = StringUtils.isBlank(collectionName) ? dataCollectionEntityMgr.findDefaultCollection().getName()
                : collectionName;
        StatisticsContainer container = statisticsContainerDao.findInMasterSegment(collectionName, version);
        return copyStatisticsToStatsCubes(container);
    }

    // TODO: deprecating in M20
    private StatisticsContainer copyStatisticsToStatsCubes(StatisticsContainer container) {
        if (container == null) {
            return null;
        }
        if (MapUtils.isEmpty(container.getStatsCubes())) {
            Statistics statistics = container.getStatistics();
            if (statistics != null) {
                log.info("Copying statistics " + container.getName() + " to stats cube.");
                Map<String, StatsCube> cubes = StatsCubeUtils.toStatsCubes(statistics);
                container.setStatsCubes(cubes);
                update(container);
            }
            container.setStatistics(null);
        }
        return container;
    }

}
