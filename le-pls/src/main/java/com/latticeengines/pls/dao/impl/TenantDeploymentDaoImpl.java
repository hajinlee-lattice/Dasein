package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.pls.dao.TenantDeploymentDao;

@Component("tenantDeploymentDao")
public class TenantDeploymentDaoImpl extends BaseDaoImpl<TenantDeployment> implements TenantDeploymentDao {

    @Override
    protected Class<TenantDeployment> getEntityClass() {
        return TenantDeployment.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public TenantDeployment findByTenantId(long tenantId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<TenantDeployment> entityClz = getEntityClass();
        String queryStr = String.format("from %s where tenant_id = :tenantId order by pid desc", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("tenantId", tenantId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }

        return (TenantDeployment)list.get(0);
    }

}
