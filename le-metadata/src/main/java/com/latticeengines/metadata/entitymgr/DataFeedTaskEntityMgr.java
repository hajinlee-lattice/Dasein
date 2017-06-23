package com.latticeengines.metadata.entitymgr;

import java.util.Date;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.DataFeedTask.Status;
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

    // boolean dataFeedTaskExist(String dataFeedType, String entity);

    void createDataFeedTask(DataFeedTask dataFeedTask);

    DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, Long dataFeedId);

    DataFeedTask getDataFeedTask(Long pid);

    void deleteByTaskId(Long taskId);

    void updateDataFeedTask(DataFeedTask dataFeedTask);

    void update(DataFeedTask task, Table importData, Date startTime);

    void update(DataFeedTask dataFeedTask, Table importData);

    void update(DataFeedTask datafeedTask, Table importData, Status status, Date lastImported);

}
