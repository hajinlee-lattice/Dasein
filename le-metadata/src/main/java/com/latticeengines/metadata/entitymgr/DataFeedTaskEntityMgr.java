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

    void registerExtract(DataFeedTask dataFeedTask, String tableName, Extract extract);

    void addTableToQueue(DataFeedTask dataFeedTask, Table table);

//    boolean dataFeedTaskExist(String dataFeedType, String entity);

    void createDataFeedTask(DataFeedTask dataFeedTask);

    DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, Long dataFeedId);

    DataFeedTask getDataFeedTask(Long pid);

    void deleteByTaskId(Long taskId);

    void updateDataFeedTask(DataFeedTask dataFeedTask);

}
