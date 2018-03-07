package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.apps.cdl.service.StatisticsContainerService;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

@Component("statisticsContainer")
public class StatisticsContainerServiceImpl implements StatisticsContainerService {

    @Inject
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Override
    public StatisticsContainer findByName(String customerSpace, String statisticsName) {
        return statisticsContainerEntityMgr.findStatisticsByName(statisticsName);
    }

    @Override
    public StatisticsContainer createOrUpdate(String customerSpace, StatisticsContainer container) {
        return statisticsContainerEntityMgr.createOrUpdateStatistics(container);
    }

    @Override
    public void delete(String customerSpace, String name) {
        StatisticsContainer container = findByName(customerSpace, name);
        if (container != null) {
            statisticsContainerEntityMgr.delete(container);
        }
    }
}
