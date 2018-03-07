package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataFeedExecutionDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;

@Component("datafeedExecutionDao")
public class DataFeedExecutionDaoImpl extends BaseDaoImpl<DataFeedExecution> implements DataFeedExecutionDao {

    @Override
    protected Class<DataFeedExecution> getEntityClass() {
        return DataFeedExecution.class;
    }

}
