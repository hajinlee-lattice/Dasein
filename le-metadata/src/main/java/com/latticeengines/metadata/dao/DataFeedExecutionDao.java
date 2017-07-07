package com.latticeengines.metadata.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;

public interface DataFeedExecutionDao extends BaseDao<DataFeedExecution> {

    DataFeedExecution findConsolidatingExecution(DataFeed datafeed);

    List<DataFeedExecution> findByDataFeed(DataFeed datafeed);

}
