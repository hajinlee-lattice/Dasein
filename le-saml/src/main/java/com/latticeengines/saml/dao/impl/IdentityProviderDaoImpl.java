package com.latticeengines.saml.dao.impl;

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
}
