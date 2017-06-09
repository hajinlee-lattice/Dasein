package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;

public interface DataFeedTaskDao extends BaseDao<DataFeedTask> {

    //boolean dataFeedTaskExist(String dataFeedType, String entity);

    void createDataFeedTask(DataFeedTask dataFeedTask);

    DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, Long dataFeed);

}
