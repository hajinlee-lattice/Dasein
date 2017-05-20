package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.metadata.dao.DataFeedDao;

@Component("datafeedDao")
public class DataFeedDaoImpl extends BaseDaoImpl<DataFeed> implements DataFeedDao {

    @Override
    protected Class<DataFeed> getEntityClass() {
        return DataFeed.class;
    }

}
