package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.security.TenantStatus;

public interface DataFeedEntityMgr extends BaseEntityMgrRepository<DataFeed, Long> {

    DataFeed findByPid(Long pid);

    DataFeed findByName(String datafeedName);

    DataFeedExecution updateExecutionWithTerminalStatus(String datafeedName, DataFeedExecution.Status status,
                                                        Status datafeedStatus);

    DataFeedExecution updateExecutionWithTerminalStatus(String datafeedName, DataFeedExecution.Status status,
                                                        Status datafeedStatus, Long executionId);

    DataFeed findByNameInflatedWithAllExecutions(String datafeedName);

    DataFeed findByNameInflated(String datafeedName);

    DataFeed findDefaultFeed();

    DataFeed findDefaultFeedReadOnly();

    List<DataFeed> getAllDataFeeds();

    List<SimpleDataFeed> getAllSimpleDataFeeds();

    List<SimpleDataFeed> getSimpleDataFeedsByTenantStatus(TenantStatus status);

    DataFeed updateStatus(DataFeed dataFeed);
}
