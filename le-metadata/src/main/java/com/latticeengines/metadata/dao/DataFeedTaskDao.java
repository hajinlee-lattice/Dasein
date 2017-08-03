package com.latticeengines.metadata.dao;

import java.util.Date;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;

public interface DataFeedTaskDao extends BaseDao<DataFeedTask> {

    // boolean dataFeedTaskExist(String dataFeedType, String entity);

    void createDataFeedTask(DataFeedTask datafeedTask);

    DataFeedTask getDataFeedTask(String source, String datafeedType, String entity, Long dataFeed);

    void update(DataFeedTask datafeedTask, Date startTime);

    void update(DataFeedTask datafeedTask, Status status, Date lastImported);

}
