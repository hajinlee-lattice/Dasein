package com.latticeengines.auth.exposed.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.auth.exposed.dao.GlobalAuthTicketDao;

@Component("globalAuthTicketDao")
public class GlobalAuthTicketDaoImpl extends BaseDaoImpl<GlobalAuthTicket> implements GlobalAuthTicketDao {

    @Override
    protected Class<GlobalAuthTicket> getEntityClass() {
        return GlobalAuthTicket.class;
    }

}
