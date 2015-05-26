package com.latticeengines.admin.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.dao.BardJamsRequestDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;

@Component
public class BardJamsRequestDaoImpl extends BaseDaoImpl<BardJamsTenant> implements BardJamsRequestDao {

    @Override
    protected Class<BardJamsTenant> getEntityClass() {
        return BardJamsTenant.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public BardJamsTenant findByTenant(String tenant) {
        Session session = getSessionFactory().getCurrentSession();
        Class<BardJamsTenant> entityClz = getEntityClass();
        String queryStr = String.format("from %s where tenant = '%s'", entityClz.getSimpleName(), tenant);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (BardJamsTenant) list.get(0);
    }

}