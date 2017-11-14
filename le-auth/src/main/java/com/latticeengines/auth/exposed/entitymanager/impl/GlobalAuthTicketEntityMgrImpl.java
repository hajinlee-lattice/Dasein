package com.latticeengines.auth.exposed.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.auth.exposed.dao.GlobalAuthTicketDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;

import java.util.Date;

@Component("globalAuthTicketEntityMgr")
public class GlobalAuthTicketEntityMgrImpl extends BaseEntityMgrImpl<GlobalAuthTicket> implements
        GlobalAuthTicketEntityMgr {

    @Autowired
    private GlobalAuthTicketDao gaTicketDao;

    @Override
    public BaseDao<GlobalAuthTicket> getDao() {
        return gaTicketDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public GlobalAuthTicket findByTicket(String ticket) {
        return gaTicketDao.findByField("Ticket", ticket);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(GlobalAuthTicket gaTicket) {
    	    getDao().create(gaTicket);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void delete(GlobalAuthTicket gaTicket) {
    	    getDao().delete(gaTicket);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void update(GlobalAuthTicket gaTicket) {
        gaTicket.setLastModificationDate(new Date(System.currentTimeMillis()));
        getDao().update(gaTicket);
    }
}
