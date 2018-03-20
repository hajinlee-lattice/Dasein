package com.latticeengines.apps.cdl.repository.writer;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;

public interface DataFeedRepository extends BaseJpaRepository<DataFeed, Long> {

    DataFeed findByName(String datafeedName);

    DataFeed findByDataCollection(DataCollection dataCollection);
}
