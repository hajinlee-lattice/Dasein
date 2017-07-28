package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;

public interface DataFeedService {

    DataFeedExecution startExecution(String customerSpace, String datafeedName);

    DataFeed findDataFeedByName(String customerSpace, String datafeedName);

    DataFeedExecution finishExecution(String customerSpace, String datafeedName, String initialDataFeedStatus);

    DataFeed createDataFeed(String customerSpace, String collectionName, DataFeed datafeed);

    DataFeed getOrCreateDataFeed(String customerSpace);

    void updateDataFeed(String customerSpace, String datafeedName, String status);

    DataFeedExecution failExecution(String customerSpace, String datafeedName, String initialDataFeedStatus);

    DataFeedExecution updateExecutionWorkflowId(String customerSpace, String datafeedName, Long workflowId);

    DataFeedExecution retryLatestExecution(String customerSpace, String datafeedName);

    DataFeedProfile startProfile(String customerSpace, String datafeedName);

    DataFeedProfile updateProfileWorkflowId(String customerSpace, String datafeedName, Long workflowId);

    void resetImport(String customerSpace, String datafeedName);

}
