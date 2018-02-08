package com.latticeengines.metadata.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;

public interface DataFeedDao extends BaseDao<DataFeed> {

    DataFeed findDefaultFeed(String collectionName);

    List<SimpleDataFeed> findAllSimpleDataFeeds();
}
