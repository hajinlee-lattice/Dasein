package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;

public interface DataFeedTaskTableEntityMgr extends BaseEntityMgrRepository<DataFeedTaskTable, Long> {

    Table peekFirstDataTable(DataFeedTask datafeedTask);

    Table pollFirstDataTable(DataFeedTask datafeedTask);

    int countDataFeedTaskTables(DataFeedTask datafeedTask);

    List<DataFeedTaskTable> getDataFeedTaskTables(DataFeedTask datafeedTask);

    void deleteDataFeedTaskTables(DataFeedTask dataFeedTask);

    List<DataFeedTaskTable> getInflatedDataFeedTaskTables(DataFeedTask datafeedTask);

}
