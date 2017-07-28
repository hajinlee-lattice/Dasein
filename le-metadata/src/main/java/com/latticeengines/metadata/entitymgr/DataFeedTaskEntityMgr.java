package com.latticeengines.metadata.entitymgr;

import java.util.Date;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;

public interface DataFeedTaskEntityMgr extends BaseEntityMgr<DataFeedTask> {

    Table pollFirstDataTable(DataFeedTask task);

    Table peekFirstDataTable(DataFeedTask task);

    int getDataTableSize(DataFeedTask task);

    void addImportDataTableToQueue(DataFeedTask task);

    void clearTableQueue();

    void clearTableQueuePerTask(DataFeedTask task);

    void registerExtract(DataFeedTask dataFeedTask, String tableName, Extract extract);

    void registerExtracts(DataFeedTask datafeedTask, String tableName, List<Extract> extracts);

    void addTableToQueue(DataFeedTask dataFeedTask, Table table);

    DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, Long dataFeedId);

    DataFeedTask getDataFeedTask(Long pid);

    DataFeedTask getDataFeedTask(String uniqueId);

    void deleteByTaskId(Long taskId);

    void updateDataFeedTask(DataFeedTask dataFeedTask);

    void update(DataFeedTask task, Table importData, Date startTime);

    void update(DataFeedTask dataFeedTask, Table importData);

    void update(DataFeedTask datafeedTask, Table importData, Status status, Date lastImported);

    List<DataFeedTaskTable> getDataTables(DataFeedTask datafeedTask);

    List<Extract> getExtractsPendingInQueue(DataFeedTask task);

}
