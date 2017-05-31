package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataFeedTaskTable;
import com.latticeengines.domain.exposed.metadata.Table;

public interface DataFeedTaskTableDao extends BaseDao<DataFeedTaskTable> {

    Table pollFirstDataTable(Long taskPid);

    Table peekFirstDataTable(Long taskPid);
}
