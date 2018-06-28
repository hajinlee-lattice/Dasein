package com.latticeengines.apps.cdl.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;

public interface DataFeedDao extends BaseDao<DataFeed> {
    DataFeed updateStatus(DataFeed dataFeed);
}
