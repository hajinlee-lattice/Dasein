package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;

public interface DataFeedTaskService {

    void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask);

    DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity);

    DataFeedTask getDataFeedTask(String customerSpace, Long taskId);

    void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask);

    void registerExtract(String customerSpace, Long taskId, String tableName, Extract extract);
}
