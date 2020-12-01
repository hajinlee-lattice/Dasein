package com.latticeengines.apps.cdl.service;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AttributeLimit;
import com.latticeengines.domain.exposed.cdl.DataLimit;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.security.TenantStatus;

public interface DataFeedService {

    DataFeedExecution startExecution(String customerSpace, String datafeedName, DataFeedExecutionJobType jobTyp,
                                     long jobId, List<Long> actionIds);

    DataFeed findDataFeedByName(String customerSpace, String datafeedName);

    DataFeedExecution finishExecution(String customerSpace, String datafeedName, String initialDataFeedStatus);

    DataFeedExecution finishExecution(String customerSpace, String datafeedName, String initialDataFeedStatus,
                                      Long executionId);

    DataFeed createDataFeed(String customerSpace, String collectionName, DataFeed datafeed);

    DataFeed getOrCreateDataFeed(String customerSpace);

    DataFeed getDefaultDataFeed(String customerSpace);

    void updateDataFeedDrainingStatus(String customerSpace, String drainingStatusStr);

    void updateDataFeedMaintenanceMode(String customerSpace, boolean maintenanceMode);

    void updateDataFeed(String customerSpace, String datafeedName, String status);

    void updateDataFeedNextInvokeTime(String customerSpace, Date time);

    DataFeedExecution getLatestExecution(String customerSpace, String datafeedName, DataFeedExecutionJobType jobType);

    DataFeedExecution failExecution(String customerSpace, String datafeedName, String initialDataFeedStatus);

    DataFeedExecution failExecution(String customerSpace, String datafeedName, String initialDataFeedStatus,
                                    Long executionId);

    DataFeedExecution updateExecutionWorkflowId(String customerSpace, String datafeedName, Long workflowId);

    DataFeed updateEarliestLatestTransaction(String customerSpace, String datafeedName, Integer earliestDayPeriod,
            Integer latestDayPeriod);

    DataFeed rebuildTransaction(String customerSpace, String datafeedName, Boolean isRebuild);

    List<DataFeed> getAllDataFeeds();

    List<SimpleDataFeed> getAllSimpleDataFeeds();

    List<SimpleDataFeed> getSimpleDataFeeds(TenantStatus status, String version);

    List<DataFeed> getDataFeeds(TenantStatus status, String version);

    List<DataFeed> getDataFeedsBySchedulingGroup(TenantStatus status, String version, String schedulingGroup);

    Long lockExecution(String customerSpace, String datafeedName, DataFeedExecutionJobType jobType);

    Long restartExecution(String id, String datafeedName, DataFeedExecutionJobType jobType);

    Boolean unblockPA(String customerSpace, Long workflowId);

    void updateDataFeedScheduleTime(String customerSpace, Boolean scheduleNow, ProcessAnalyzeRequest request);

    DataLimit getDataQuotaLimitMap(CustomerSpace customerSpace);

    Boolean increasedRetryCount(String customerSpace);

    AttributeLimit getAttributeQuotaLimit(String customerSpace);

    DataFeedExecution getLatestExecution(String customerSpace, DataFeedExecutionJobType jobType);
}
