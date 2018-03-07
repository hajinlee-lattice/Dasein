package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataFeedDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;

@Component("datafeedDao")
public class DataFeedDaoImpl extends BaseDaoImpl<DataFeed> implements DataFeedDao {

    @Override
    protected Class<DataFeed> getEntityClass() {
        return DataFeed.class;
    }

}
