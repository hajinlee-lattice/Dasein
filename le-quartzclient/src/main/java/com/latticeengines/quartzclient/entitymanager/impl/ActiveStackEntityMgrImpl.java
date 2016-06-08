package com.latticeengines.quartzclient.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.quartz.ActiveStack;
import com.latticeengines.quartzclient.dao.ActiveStackDao;
import com.latticeengines.quartzclient.entitymanager.ActiveStackEntityMgr;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component("activeStackEntityMgr")
public class ActiveStackEntityMgrImpl extends BaseEntityMgrImpl<ActiveStack> implements
        ActiveStackEntityMgr {

    @Autowired
    private ActiveStackDao activeStackDao;

    @Override
    public BaseDao<ActiveStack> getDao() {
        return activeStackDao;
    }

    @Override
    @Transactional(value = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public String getActiveStack() {
        return activeStackDao.getActiveStack();
    }

}
