package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;

public interface DataFeedEntityMgr extends BaseEntityMgr<DataFeed> {

    DataFeedExecution startExecution(String datafeedName);

    DataFeed findByName(String datafeedName);

    DataFeedExecution updateExecutionWithTerminalStatus(String datafeedName, DataFeedExecution.Status status,
            Status datafeedStatus);

    DataFeed findByNameInflatedWithAllExecutions(String datafeedName);

    DataFeed findByNameInflated(String datafeedName);

    DataFeed findDefaultFeed();

    DataFeed findDefaultFeedReadOnly();

    DataFeedExecution retryLatestExecution(String datafeedName);

    DataFeedProfile startProfile(String datafeedName);

    List<DataFeed> getAllDataFeeds();
}
