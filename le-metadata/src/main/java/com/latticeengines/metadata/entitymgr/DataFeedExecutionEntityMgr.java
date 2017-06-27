package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;

public interface DataFeedExecutionEntityMgr extends BaseEntityMgr<DataFeedExecution> {

    DataFeedExecution findByExecutionId(Long executionId);

    DataFeedExecution findConsolidatingExecution(DataFeed datafeed);

    List<DataFeedExecution> findByDataFeed(DataFeed datafeed);

}
