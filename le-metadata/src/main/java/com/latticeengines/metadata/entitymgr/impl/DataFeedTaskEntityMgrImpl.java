package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.dao.DataFeedTaskDao;
import com.latticeengines.metadata.dao.DataFeedTaskTableDao;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;

@Component("datafeedTaskEntityMgr")
public class DataFeedTaskEntityMgrImpl extends BaseEntityMgrImpl<DataFeedTask> implements DataFeedTaskEntityMgr {

    @Autowired
    private DataFeedTaskDao datafeedTaskDao;

    @Autowired
    private DataFeedTaskTableDao datafeedTaskTableDao;

    @Override
    public BaseDao<DataFeedTask> getDao() {
        return datafeedTaskDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Table peekFirstDataTable(Long taskPid) {
        return datafeedTaskTableDao.peekFirstDataTable(taskPid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Table pollFirstDataTable(Long taskPid) {
        return datafeedTaskTableDao.pollFirstDataTable(taskPid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public int getDataTableSize(Long taskPid) {
        DataFeedTask task = this.findByField("pid", taskPid);
        HibernateUtils.inflateDetails(task);
        return task.getTables().size();
    }

}
