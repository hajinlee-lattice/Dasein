package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;

public interface DataFeedExecutionEntityMgr extends BaseEntityMgrRepository<DataFeedExecution, Long> {

    DataFeedExecution findByPid(Long executionId);

    List<DataFeedExecution> findByDataFeed(DataFeed datafeed);

    List<DataFeedExecution> findActiveExecutionByDataFeedAndJobType(DataFeed dataFeed,
                                                                    DataFeedExecutionJobType jobType);

    void updateImports(DataFeedExecution execution);

    DataFeedExecution findFirstByDataFeedAndJobTypeOrderByPidDesc(DataFeed datafeed, DataFeedExecutionJobType jobType);

    int countByDataFeedAndJobType(DataFeed datafeed, DataFeedExecutionJobType jobType);

    DataFeedExecution findByStatusAndWorkflowId(DataFeedExecution.Status status, Long workflowId);

    DataFeedExecution updateStatus(DataFeedExecution execution);

    DataFeedExecution updateRetryCount(DataFeedExecution execution);
}
