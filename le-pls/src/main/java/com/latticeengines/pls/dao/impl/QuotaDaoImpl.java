package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.pls.dao.QuotaDao;

@Component("quotaDao")
public class QuotaDaoImpl extends BaseDaoImpl<Quota> implements QuotaDao {

    @Override
    protected Class<Quota> getEntityClass() {
        return Quota.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Quota findQuotaByQuotaId(String quotaId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Quota> entityClz = getEntityClass();
        String queryStr = String.format("from %s where ID = :id",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("id", quotaId);
        List quotas = query.list();
        if (quotas.size() == 0) {
            return null;
        }
        return (Quota) quotas.get(0);
    }
}
