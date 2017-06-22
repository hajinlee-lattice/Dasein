package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface StatisticsContainerDao extends BaseDao<StatisticsContainer> {

    StatisticsContainer findInSegment(String segmentName, String modelId);

    StatisticsContainer findInMasterSegment(String collectionName, String modelId);

}
