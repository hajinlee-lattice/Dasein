package com.latticeengines.apps.cdl.dao;

import java.util.Date;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;

public interface DataFeedTaskDao extends BaseDao<DataFeedTask> {

    void createDataFeedTask(DataFeedTask datafeedTask);

    void update(DataFeedTask datafeedTask, Date startTime);

    void update(DataFeedTask datafeedTask, Status status, Date lastImported);

}
