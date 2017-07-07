package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;

public interface DataFeedExecutionEntityMgr extends BaseEntityMgr<DataFeedExecution> {

    DataFeedExecution findByExecutionId(Long executionId);

    DataFeedExecution findConsolidatingExecution(DataFeed datafeed);

    List<DataFeedExecution> findByDataFeed(DataFeed datafeed);

}
