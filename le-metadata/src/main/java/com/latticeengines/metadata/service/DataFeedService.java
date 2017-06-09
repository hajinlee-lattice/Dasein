package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;

public interface DataFeedService {

    DataFeedExecution startExecution(String customerSpace, String datafeedName);

    DataFeed findDataFeedByName(String customerSpace, String datafeedName);

    DataFeedExecution finishExecution(String customerSpace, String datafeedName);

    DataFeed createDataFeed(String customerSpace, DataFeed datafeed);

    DataFeedExecution failExecution(String customerSpace, String datafeedName);

    DataFeedExecution updateExecutionWorkflowId(String customerSpace, String datafeedName, Long workflowId);

}
