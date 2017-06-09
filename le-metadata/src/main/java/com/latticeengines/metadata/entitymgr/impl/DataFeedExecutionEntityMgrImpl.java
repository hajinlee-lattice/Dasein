package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.metadata.dao.DataFeedExecutionDao;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;

@Component("datafeedExecutionEntityMgr")
public class DataFeedExecutionEntityMgrImpl extends BaseEntityMgrImpl<DataFeedExecution>
        implements DataFeedExecutionEntityMgr {

    @Autowired
    private DataFeedExecutionDao datafeedExecutionDao;

    @Override
    public BaseDao<DataFeedExecution> getDao() {
        return datafeedExecutionDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public DataFeedExecution findByExecutionId(long executionId) {
        return findByField("pid", executionId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public DataFeedExecution findConsolidatingExecution(DataFeed datafeed) {
        return datafeedExecutionDao.findConsolidatingExecution(datafeed);
    }

    @Override
    public List<DataFeedExecution> findByDataFeed(DataFeed datafeed) {
        return datafeedExecutionDao.findByDataFeed(datafeed);
    }

}
