package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;

public interface DataFeedExecutionDao extends BaseDao<DataFeedExecution> {

    DataFeedExecution findConsolidatingExecution(DataFeed datafeed);

}
