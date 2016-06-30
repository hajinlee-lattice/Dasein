package com.latticeengines.saml.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.saml.IdentityProvider;

public interface IdentityProviderDao extends BaseDao<IdentityProvider> {
    List<IdentityProvider> findByTenantId(String tenantId);
}
