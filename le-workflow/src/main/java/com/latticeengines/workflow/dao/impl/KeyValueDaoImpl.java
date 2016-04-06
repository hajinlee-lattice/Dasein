package com.latticeengines.workflow.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.workflow.exposed.dao.KeyValueDao;

@Component("keyValueDao")
public class KeyValueDaoImpl extends BaseDaoImpl<KeyValue> implements KeyValueDao {

    @Override
    protected Class<KeyValue> getEntityClass() {
        return KeyValue.class;
    }

    @SuppressWarnings({"unchecked" })
    @Override
    public List<KeyValue> findByTenantId(long tenantId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<KeyValue> entityClz = getEntityClass();
        String queryStr = String.format("from %s where tenantId = '%s'", entityClz.getSimpleName(), tenantId);
        Query query = session.createQuery(queryStr);
        return query.list();
    }
}
