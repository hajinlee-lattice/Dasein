package com.latticeengines.metadata.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.DataFeedTaskTable;
import com.latticeengines.domain.exposed.metadata.Table;

public interface DataFeedTaskTableDao extends BaseDao<DataFeedTaskTable> {

    Table pollFirstDataTable(DataFeedTask task);

    Table peekFirstDataTable(DataFeedTask task);

    List<DataFeedTaskTable> getDataFeedTaskTables(DataFeedTask task);

}
