package com.latticeengines.saml.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.dao.IdentityProviderDao;

@Component("identityProviderDao")
public class IdentityProviderDaoImpl extends BaseDaoImpl<IdentityProvider> implements IdentityProviderDao {

    @Override
    protected Class<IdentityProvider> getEntityClass() {
        return IdentityProvider.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<IdentityProvider> findByTenantId(String tenantId) {
        Session session = sessionFactory.getCurrentSession();
        Class<IdentityProvider> entityClz = getEntityClass();
        String queryStr = String
                .format("from %s where globalAuthTenant.id = '%s'", entityClz.getSimpleName(), tenantId);
        Query query = session.createQuery(queryStr);
        return (List<IdentityProvider>) query.list();
    }
}
