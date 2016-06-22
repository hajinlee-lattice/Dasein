package com.latticeengines.security.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthSession;
import com.latticeengines.security.dao.GlobalAuthSessionDao;
import com.latticeengines.security.entitymanager.GlobalAuthSessionEntityMgr;

@Component("globalAuthSessionEntityMgr")
public class GlobalAuthSessionEntityMgrImpl extends BaseEntityMgrImpl<GlobalAuthSession> implements
        GlobalAuthSessionEntityMgr {

    @Autowired
    private GlobalAuthSessionDao gaSessionDao;

    @Override
    public BaseDao<GlobalAuthSession> getDao() {
        return gaSessionDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthSession findByTicketId(Long ticketId) {
        return gaSessionDao.findByField("Ticket_ID", ticketId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(GlobalAuthSession gaSession) {
        super.create(gaSession);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void delete(GlobalAuthSession gaSession) {
        super.delete(gaSession);
    }

}
