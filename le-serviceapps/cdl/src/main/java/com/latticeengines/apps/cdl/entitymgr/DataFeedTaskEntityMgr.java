package com.latticeengines.apps.cdl.entitymgr;

import java.util.Date;
import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.SourceInfo;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskSummary;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;

public interface DataFeedTaskEntityMgr extends BaseEntityMgrRepository<DataFeedTask, Long> {

    void clearTableQueue();

    void clearTableQueuePerTask(DataFeedTask task);

    List<String> registerExtract(DataFeedTask dataFeedTask, String tableName, Extract extract);

    List<String> registerExtracts(DataFeedTask datafeedTask, String tableName, List<Extract> extracts);

    void addTableToQueue(DataFeedTask dataFeedTask, Table table);

    void addTableToQueue(String dataFeedTaskUniqueId, String tableName);

    void addTablesToQueue(String dataFeedTaskUniqueId, List<String> tableNames);

    DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, DataFeed datafeed);

    DataFeedTask getDataFeedTask(String source, String dataFeedType, DataFeed datafeed);

    DataFeedTask getDataFeedTask(Long pid);

    DataFeedTask getDataFeedTask(String uniqueId);

    DataFeedTask getDataFeedTaskByTaskName(String taskName, DataFeed dataFeed, Boolean withTemplate);

    DataFeedTask getDataFeedTask(DataFeed dataFeed, String sourceId);

    List<DataFeedTask> getDataFeedTaskWithSameEntity(String entity, DataFeed datafeed);

    List<DataFeedTask> getDataFeedTaskUnderDataFeed(DataFeed dataFeed);

    List<DataFeedTask> getDataFeedTaskWithSameEntityExcludeOne(String entity, DataFeed datafeed,
                                                               String excludeSource, String excludeFeedType);

    List<DataFeedTask> getDataFeedTaskByUniqueIds(List<String> uniqueIds);

    void deleteByTaskId(Long taskId);

    void updateDataFeedTask(DataFeedTask dataFeedTask, boolean updateTaskOnly);

    void update(DataFeedTask task, Date startTime);

    void update(DataFeedTask datafeedTask, Status status, Date lastImported);

    List<Extract> getExtractsPendingInQueue(DataFeedTask task);

    List<DataFeedTaskTable> getInflatedDataFeedTaskTables(DataFeedTask task);

    void setDeleted(Long pid, Boolean deleted);

    void setS3ImportStatusBySource(Long pid, DataFeedTask.S3ImportStatus status);

    List<SourceInfo> getSourcesBySystemPid(Long systemPid, Pageable pageable);

    Long countSourcesBySystemPid(Long systemPid);

    SourceInfo getSourceBySourceIdAndDataFeed(String sourceId, DataFeed dataFeed);

    List<DataFeedTaskSummary> getSummaryBySourceAndDataFeed(String source, String customerSpace);

    List<DataFeedTaskSummary> getSummaryByDataFeed(String customerSpace);

    boolean existsBySourceAndFeedType(String source, String feedType, String customerSpace);

}
