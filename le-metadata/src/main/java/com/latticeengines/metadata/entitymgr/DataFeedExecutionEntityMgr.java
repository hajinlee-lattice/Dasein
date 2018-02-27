package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;

public interface DataFeedExecutionEntityMgr extends BaseEntityMgr<DataFeedExecution> {

    DataFeedExecution findByPid(Long executionId);

    List<DataFeedExecution> findByDataFeed(DataFeed datafeed);

    void updateImports(DataFeedExecution execution);

    DataFeedExecution findFirstByDataFeedAndJobTypeOrderByPidDesc(DataFeed datafeed, DataFeedExecutionJobType jobType);

    int countByDataFeedAndJobType(DataFeed datafeed, DataFeedExecutionJobType jobType);

}
