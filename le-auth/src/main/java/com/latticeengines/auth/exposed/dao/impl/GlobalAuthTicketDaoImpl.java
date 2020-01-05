package com.latticeengines.auth.exposed.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthTicketDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthSession;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;

@Component("globalAuthTicketDao")
public class GlobalAuthTicketDaoImpl extends BaseDaoImpl<GlobalAuthTicket> implements GlobalAuthTicketDao {

    @Override
    protected Class<GlobalAuthTicket> getEntityClass() {
        return GlobalAuthTicket.class;
    }

    @Override
    public List<GlobalAuthTicket> findTicketsByUserIdAndTenant(Long userId, GlobalAuthTenant tenantData) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthTicket> entityClz = getEntityClass();
        Class<GlobalAuthSession> sessionClz = GlobalAuthSession.class;
        long end = System.currentTimeMillis() - GlobalAuthTicketDao.TicketInactivityTimeoutInMinute;
        String queryStr = String.format("from %s WHERE userId = %d AND UNIX_TIMESTAMP( lastAccessDate ) <= :lastAccessDate and " +
                        "pid in (select ticketId from %s where tenantId=%d and userId=%d)",
                entityClz.getSimpleName(), userId, sessionClz.getSimpleName(), tenantData.getPid(), userId);
        Query query = session.createQuery(queryStr);
        query.setParameter("lastAccessDate", end);
        return query.list();
    }
}
