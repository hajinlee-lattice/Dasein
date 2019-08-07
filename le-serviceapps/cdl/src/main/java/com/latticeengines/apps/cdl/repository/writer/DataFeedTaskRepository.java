package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

public interface DataFeedTaskRepository extends BaseJpaRepository<DataFeedTask, Long> {

    DataFeedTask findBySourceAndFeedTypeAndEntityAndDataFeed(String source, String datafeedType, String entity,
                                                             DataFeed dataFeed);

    DataFeedTask findBySourceAndFeedTypeAndDataFeed(String source, String datafeedType, DataFeed dataFeed);

    List<DataFeedTask> findByEntityAndDataFeed(String entity, DataFeed dataFeed);

    List<DataFeedTask> findByUniqueIdIn(List<String> uniqueIds);

    DataFeedTask findByUniqueId(String uniqueId);
}
