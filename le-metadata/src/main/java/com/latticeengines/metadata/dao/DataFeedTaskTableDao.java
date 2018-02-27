package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;

public interface DataFeedTaskTableDao extends BaseDao<DataFeedTaskTable> {

    void deleteDataFeedTaskTables(DataFeedTask task);

}
