package com.latticeengines.security.exposed.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.dao.TenantDao;

@Component("tenantDao")
public class TenantDaoImpl extends BaseDaoImpl<Tenant> implements TenantDao {

    @Override
    protected Class<Tenant> getEntityClass() {
        return Tenant.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Tenant findByTenantId(String tenantId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Tenant> entityClz = getEntityClass();
        String queryStr = String.format("from %s where id = '%s'", entityClz.getSimpleName(), tenantId);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (Tenant) list.get(0);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Tenant findByTenantName(String tenantName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Tenant> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = '%s'", entityClz.getSimpleName(), tenantName);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (Tenant) list.get(0);
    }

}
