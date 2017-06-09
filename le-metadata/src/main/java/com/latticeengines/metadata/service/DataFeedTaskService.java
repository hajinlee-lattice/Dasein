package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;

public interface DataFeedTaskService {

    void createDataFeedTask(String customerSpace, String dataFeedName, DataFeedTask dataFeedTask);

    DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity, String dataFeedName);

    DataFeedTask getDataFeedTask(Long taskId);

    void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask);

    void registerExtract(String customerSpace, Long taskId, Extract extract);
}
