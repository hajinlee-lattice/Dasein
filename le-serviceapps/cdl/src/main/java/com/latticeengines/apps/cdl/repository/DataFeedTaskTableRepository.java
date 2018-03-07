package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;

public interface DataFeedTaskTableRepository extends BaseJpaRepository<DataFeedTaskTable, Long> {

    DataFeedTaskTable findFirstByDataFeedTaskOrderByPidAsc(DataFeedTask datafeedTask);

    List<DataFeedTaskTable> findByDataFeedTask(DataFeedTask datafeedTask);

    int countByDataFeedTask(DataFeedTask datafeedTask);
}
