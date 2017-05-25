package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
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

}
