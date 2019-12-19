package com.latticeengines.apps.cdl.dao;

import java.util.Date;
import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;

public interface DataFeedTaskDao extends BaseDao<DataFeedTask> {

    void createDataFeedTask(DataFeedTask datafeedTask);

    void update(DataFeedTask datafeedTask, Date startTime);

    void update(DataFeedTask datafeedTask, Status status, Date lastImported);

    List<DataFeedTask> findByEntityAndDataFeedExcludeOne(String entity, DataFeed datafeed,
                                                         String excludeSource, String excludeFeedType);

}
