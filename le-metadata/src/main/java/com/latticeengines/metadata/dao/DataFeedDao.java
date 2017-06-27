package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.DataFeed;

public interface DataFeedDao extends BaseDao<DataFeed> {

    DataFeed findDefaultFeed(String collectionName);

}
