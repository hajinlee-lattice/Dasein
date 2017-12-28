package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;

public interface DataFeedService {

    DataFeedExecution startExecution(String customerSpace, String datafeedName);

    DataFeed findDataFeedByName(String customerSpace, String datafeedName);

    DataFeedExecution finishExecution(String customerSpace, String datafeedName, String initialDataFeedStatus);

    DataFeed createDataFeed(String customerSpace, String collectionName, DataFeed datafeed);

    DataFeed getOrCreateDataFeed(String customerSpace);

    DataFeed getDefaultDataFeed(String customerSpace);

    void updateDataFeedDrainingStatus(String customerSpace, String drainingStatusStr);

    void updateDataFeedMaintenanceMode(String customerSpace, boolean maintenanceMode);

    void updateDataFeed(String customerSpace, String datafeedName, String status);

    DataFeedExecution failExecution(String customerSpace, String datafeedName, String initialDataFeedStatus);

    DataFeedExecution updateExecutionWorkflowId(String customerSpace, String datafeedName, Long workflowId);

    DataFeedExecution retryLatestExecution(String customerSpace, String datafeedName);

    DataFeedProfile startProfile(String customerSpace, String datafeedName);

    DataFeedProfile updateProfileWorkflowId(String customerSpace, String datafeedName, Long workflowId);

    void resetImport(String customerSpace, String datafeedName);

    void resetImportByEntity(String customerSpace, String datafeedName, String entity);

    DataFeed finishProfile(String customerSpace, String datafeedName, String statusStr);

    DataFeed updateEarliestTransaction(String customerSpace, String datafeedName, Integer transactionDayPeriod);
    DataFeed rebuildTransaction(String customerSpace, String datafeedName, Boolean isRebuild);

    List<DataFeed> getAllDataFeeds();

}
