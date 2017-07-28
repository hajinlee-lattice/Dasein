package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

public interface DataFeedTaskService {

    void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask);

    DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity);

    DataFeedTask getDataFeedTask(String customerSpace, Long taskId);

    DataFeedTask getDataFeedTask(String customerSpace, String uniqueId);

    void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask);

    void registerExtract(String customerSpace, String taskUniqueId, String tableName, Extract extract);

    void registerExtracts(String customerSpace, String taskUniqueId, String tableName, List<Extract> extracts);

    List<Extract> getExtractsPendingInQueue(String customerSpace, String source, String dataFeedType, String entity);

    void resetImport(String customerSpace, DataFeedTask datafeedTask);
}
