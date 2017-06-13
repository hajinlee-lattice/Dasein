package com.latticeengines.network.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;

public interface DataFeedTaskInterface {

    Boolean dataFeedTaskExist(String customerSpace, String dataFeedType, String entity);

    void createDataFeedTask(String customerSpace, String dataFeedName, DataFeedTask dataFeedTask);

    DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity,
                                 String dataFeedName);

    DataFeedTask getDataFeedTask(String customerSpace, String id);

    void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask);

    void registerExtract(String customerSpace, String taskId, String tableName, Extract extract);

}
