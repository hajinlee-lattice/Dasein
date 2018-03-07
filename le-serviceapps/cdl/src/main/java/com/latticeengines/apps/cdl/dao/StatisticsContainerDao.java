package com.latticeengines.apps.cdl.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface StatisticsContainerDao extends BaseDao<StatisticsContainer> {

    StatisticsContainer findInSegment(String segmentName, DataCollection.Version version);

    StatisticsContainer findInMasterSegment(String collectionName, DataCollection.Version version);

}
