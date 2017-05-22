package com.latticeengines.metadata.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.metadata.service.StatisticsContainerService;

@Component("statisticsContainer")
public class StatisticsContainerServiceImpl implements StatisticsContainerService {

    @Autowired
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
