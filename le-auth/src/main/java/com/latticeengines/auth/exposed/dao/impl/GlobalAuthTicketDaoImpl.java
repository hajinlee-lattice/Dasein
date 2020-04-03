package com.latticeengines.auth.exposed.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthTicketDao;
import com.latticeengines.auth.exposed.util.SessionUtils;
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
    public List<GlobalAuthTicket> findTicketsByUserIdsAndLastAccessDateAndTenant(List<Long> userIds, GlobalAuthTenant tenantData) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthTicket> entityClz = getEntityClass();
        Class<GlobalAuthSession> sessionClz = GlobalAuthSession.class;
        long end = System.currentTimeMillis() / 1000 - SessionUtils.TicketInactivityTimeoutInMinute * 60;
        String queryStr = String.format("from %s WHERE UNIX_TIMESTAMP( lastAccessDate ) >= " +
                        ":lastAccessDate and pid in (select ticketId from %s where tenantId=%d and userId in (:userIds))",
                entityClz.getSimpleName(), sessionClz.getSimpleName(), tenantData.getPid());
        Query query = session.createQuery(queryStr);
        query.setParameter("lastAccessDate", end);
        query.setParameter("userIds", userIds);
        return query.list();
    }

    @Override
    public List<GlobalAuthTicket> findTicketsByUserIdAndExternalIssuer(Long userId, String issuer) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("FROM %s WHERE userId = :userId AND issuer = :issuer",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("userId", userId);
        query.setParameter("issuer", issuer);
        return query.list();
    }

    @Override
    public List<GlobalAuthTicket> findByUserAndTenantIdAndNotInTicketAndLastAccessDate(Long tenantId, Long userId, String ticket) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthTicket> entityClz = getEntityClass();
        Class<GlobalAuthSession> sessionClz = GlobalAuthSession.class;
        long end = System.currentTimeMillis() / 1000 - SessionUtils.TicketInactivityTimeoutInMinute * 60;
        String queryStr = String.format("from %s WHERE userId = %d AND ticket != :ticket AND UNIX_TIMESTAMP( lastAccessDate ) >= " +
                        ":lastAccessDate and pid in (select ticketId from %s where tenantId=%d and userId=%d)",
                entityClz.getSimpleName(), userId, sessionClz.getSimpleName(), tenantId, userId);
        Query query = session.createQuery(queryStr);
        query.setParameter("lastAccessDate", end);
        query.setParameter("ticket", ticket);
        return query.list();
    }

}
