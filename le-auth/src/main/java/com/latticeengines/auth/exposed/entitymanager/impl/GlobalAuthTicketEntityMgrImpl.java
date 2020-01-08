package com.latticeengines.auth.exposed.entitymanager.impl;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.auth.exposed.dao.GlobalAuthTicketDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;

@Component("globalAuthTicketEntityMgr")
public class GlobalAuthTicketEntityMgrImpl extends BaseEntityMgrImpl<GlobalAuthTicket>
        implements GlobalAuthTicketEntityMgr {

    @Inject
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

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthTicket> findTicketsByUserIdAndLastAccessDateAndTenant(Long userId, GlobalAuthTenant tenantData) {
        return gaTicketDao.findTicketsByUserIdAndLastAccessDateAndTenant(userId, tenantData);
    }
}
