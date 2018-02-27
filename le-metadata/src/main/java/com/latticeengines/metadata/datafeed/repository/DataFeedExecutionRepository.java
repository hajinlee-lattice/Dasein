package com.latticeengines.metadata.datafeed.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;

public interface DataFeedExecutionRepository extends BaseJpaRepository<DataFeedExecution, Long> {

    List<DataFeedExecution> findByDataFeed(DataFeed datafeed);

    int countByDataFeedAndJobType(DataFeed datafeed, DataFeedExecutionJobType jobType);

    DataFeedExecution findFirstByDataFeedAndJobTypeOrderByPidDesc(DataFeed datafeed, DataFeedExecutionJobType jobType);
}
