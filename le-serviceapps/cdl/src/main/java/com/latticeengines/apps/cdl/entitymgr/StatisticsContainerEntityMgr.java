package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface StatisticsContainerEntityMgr extends BaseEntityMgr<StatisticsContainer> {
    StatisticsContainer findStatisticsByName(String statisticsName);

    StatisticsContainer createOrUpdateStatistics(StatisticsContainer container);

    StatisticsContainer createStatistics(StatisticsContainer container);

    StatisticsContainer findInSegment(String segmentName, DataCollection.Version version);

    StatisticsContainer findInMasterSegment(String collectionName, DataCollection.Version version);
}
