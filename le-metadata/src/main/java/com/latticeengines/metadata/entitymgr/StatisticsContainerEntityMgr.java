package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface StatisticsContainerEntityMgr extends BaseEntityMgr<StatisticsContainer> {
    StatisticsContainer findStatisticsByName(String statisticsName);

    StatisticsContainer createOrUpdateStatistics(StatisticsContainer container);

    StatisticsContainer createStatistics(StatisticsContainer container);
}
