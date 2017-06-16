package com.latticeengines.network.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;

public interface DataFeedInterface {

    DataFeedExecution startExecution(String customerSpace, String datafeedName);

    DataFeed findDataFeedByName(String customerSpace, String datafeedName);

    DataFeedExecution finishExecution(String customerSpace, String datafeedName);

    DataFeedExecution failExecution(String customerSpace, String datafeedName);

    DataFeedExecution updateExecutionWorkflowId(String customerSpace, String datafeedName, Long workflowId);

    void updateDataFeedStatus(String customerSpace, String datafeedName, String status);
}
