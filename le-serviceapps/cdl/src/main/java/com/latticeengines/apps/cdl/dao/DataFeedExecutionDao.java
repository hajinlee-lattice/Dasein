package com.latticeengines.apps.cdl.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;

public interface DataFeedExecutionDao extends BaseDao<DataFeedExecution> {
    DataFeedExecution updateStatus(DataFeedExecution execution);

    DataFeedExecution updateRetryCount(DataFeedExecution execution);
}
