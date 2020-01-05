package com.latticeengines.auth.exposed.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthSessionDao;
import com.latticeengines.auth.exposed.dao.GlobalAuthTicketDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthSession;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;

@Component("globalAuthSessionDao")
public class GlobalAuthSessionDaoImpl extends BaseDaoImpl<GlobalAuthSession> implements GlobalAuthSessionDao {

    @Override
    protected Class<GlobalAuthSession> getEntityClass() {
        return GlobalAuthSession.class;
    }

    @Override
    public List<GlobalAuthSession> findByTicketIdsAndTenant(List<Long> ticketIds, GlobalAuthTenant globalAuthTenant) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthSession> entityClz = getEntityClass();
        long end = System.currentTimeMillis() - GlobalAuthTicketDao.TicketInactivityTimeoutInMinute;
        String queryStr = String.format("from %s WHERE tenantId = %d AND ticketId IN (:ticketIds)",
                entityClz.getSimpleName(), globalAuthTenant.getPid());
        Query query = session.createQuery(queryStr);
        query.setParameterList("ticketIds", ticketIds);
        return query.list();
    }
}
