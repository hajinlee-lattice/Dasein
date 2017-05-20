package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.metadata.dao.DataFeedExecutionDao;

@Component("datafeedExecutionDao")
public class DataFeedExecutionDaoImpl extends BaseDaoImpl<DataFeedExecution> implements DataFeedExecutionDao {

    @Override
    protected Class<DataFeedExecution> getEntityClass() {
        return DataFeedExecution.class;
    }

}
