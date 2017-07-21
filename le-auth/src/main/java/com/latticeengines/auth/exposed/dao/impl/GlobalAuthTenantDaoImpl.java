package com.latticeengines.auth.exposed.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.auth.exposed.dao.GlobalAuthTenantDao;

@Component("globalAuthTenantDao")
public class GlobalAuthTenantDaoImpl extends BaseDaoImpl<GlobalAuthTenant> implements
        GlobalAuthTenantDao {

    @Override
    protected Class<GlobalAuthTenant> getEntityClass() {
        return GlobalAuthTenant.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<GlobalAuthTenant> findTenantNotInTenantRight(GlobalAuthUser user) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthTenant> entityClz = getEntityClass();
        String queryStr = String.format("from %s tenant where tenant.pid not in "
                + "(select gaTenant.pid from %s as gaTenant inner join gaTenant.gaUserTenantRights as userTenantRight where userTenantRight.globalAuthUser = :user)",
                entityClz.getSimpleName(), entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("user", user);
        return query.list();
    }

}
