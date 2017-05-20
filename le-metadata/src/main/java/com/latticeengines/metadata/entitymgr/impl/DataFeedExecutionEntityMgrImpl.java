package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
}
