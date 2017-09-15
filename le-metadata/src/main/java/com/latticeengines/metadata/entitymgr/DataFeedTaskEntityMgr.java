package com.latticeengines.metadata.entitymgr;

import java.util.Date;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;

public interface DataFeedTaskEntityMgr extends BaseEntityMgr<DataFeedTask> {

    Table pollFirstDataTable(DataFeedTask task);

    Table peekFirstDataTable(DataFeedTask task);

    int getDataTableSize(DataFeedTask task);

    void clearTableQueue();

    void clearTableQueuePerTask(DataFeedTask task);

    void registerExtract(DataFeedTask dataFeedTask, String tableName, Extract extract);

    void registerExtracts(DataFeedTask datafeedTask, String tableName, List<Extract> extracts);

    void addTableToQueue(DataFeedTask dataFeedTask, Table table);

    DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, Long dataFeedId);

    DataFeedTask getDataFeedTask(Long pid);

    DataFeedTask getDataFeedTask(String uniqueId);

    List<DataFeedTask> getDataFeedTaskWithSameEntity(String entity, Long dataFeedId);

    void deleteByTaskId(Long taskId);

    void updateDataFeedTask(DataFeedTask dataFeedTask);

    void update(DataFeedTask task, Date startTime);

    void update(DataFeedTask datafeedTask, Status status, Date lastImported);

    List<DataFeedTaskTable> getDataTables(DataFeedTask datafeedTask);

    List<Extract> getExtractsPendingInQueue(DataFeedTask task);

}
