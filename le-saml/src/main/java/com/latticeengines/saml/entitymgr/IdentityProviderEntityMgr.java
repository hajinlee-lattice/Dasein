package com.latticeengines.saml.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.saml.IdentityProvider;

import java.util.List;

public interface IdentityProviderEntityMgr extends BaseEntityMgr<IdentityProvider> {
    List<IdentityProvider> findByTenantId(String tenantId);
}
