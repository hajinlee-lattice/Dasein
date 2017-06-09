package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;

public interface DataFeedTaskEntityMgr extends BaseEntityMgr<DataFeedTask> {

    Table pollFirstDataTable(DataFeedTask task);

    Table peekFirstDataTable(DataFeedTask task);

    int getDataTableSize(DataFeedTask task);

    void addImportDataTableToQueue(DataFeedTask task);

    void clearTableQueue();

    void registerExtract(DataFeedTask dataFeedTask, Extract extract);

    void addTableToQueue(DataFeedTask dataFeedTask, Table table);

}
