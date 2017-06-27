package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;

public interface DataFeedEntityMgr extends BaseEntityMgr<DataFeed> {

    DataFeedExecution startExecution(String datafeedName);

    DataFeed findByName(String datafeedName);

    DataFeedExecution updateExecutionWithTerminalStatus(String datafeedName, DataFeedExecution.Status status,
            Status datafeedStatus);

    DataFeed findByNameInflatedWithAllExecutions(String datafeedName);

    DataFeed findByNameInflated(String datafeedName);

    DataFeedExecution retryLatestExecution(String datafeedName);

}
