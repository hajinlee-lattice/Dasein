package com.latticeengines.saml.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.saml.IdentityProvider;

public interface IdentityProviderEntityMgr extends BaseEntityMgr<IdentityProvider> {
    List<IdentityProvider> findByTenantId(String tenantId);

    IdentityProvider findByEntityId(String entityId);
}
