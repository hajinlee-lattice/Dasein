package com.latticeengines.metadata.entitymgr;

import java.util.Date;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;

public interface DataFeedTaskEntityMgr extends BaseEntityMgrRepository<DataFeedTask, Long> {

    void clearTableQueue();

    void clearTableQueuePerTask(DataFeedTask task);

    void registerExtract(DataFeedTask dataFeedTask, String tableName, Extract extract);

    void registerExtracts(DataFeedTask datafeedTask, String tableName, List<Extract> extracts);

    void addTableToQueue(DataFeedTask dataFeedTask, Table table);

    DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, DataFeed datafeed);

    DataFeedTask getDataFeedTask(Long pid);

    DataFeedTask getDataFeedTask(String uniqueId);

    List<DataFeedTask> getDataFeedTaskWithSameEntity(String entity, DataFeed datafeed);

    void deleteByTaskId(Long taskId);

    void updateDataFeedTask(DataFeedTask dataFeedTask);

    void update(DataFeedTask task, Date startTime);

    void update(DataFeedTask datafeedTask, Status status, Date lastImported);

    List<Extract> getExtractsPendingInQueue(DataFeedTask task);

    List<DataFeedTaskTable> getInflatedDataFeedTaskTables(DataFeedTask task);

}
