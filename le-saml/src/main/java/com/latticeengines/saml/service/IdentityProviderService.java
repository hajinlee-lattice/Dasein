package com.latticeengines.saml.service;

import java.util.List;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.domain.exposed.saml.IdpMetadataValidationResponse;
import com.latticeengines.domain.exposed.saml.SamlConfigMetadata;
import com.latticeengines.domain.exposed.security.Tenant;

public interface IdentityProviderService {
    void create(IdentityProvider identityProvider);

    void delete(String entityId);

    IdentityProvider find(String entityId);

    List<IdentityProvider> findAll();

    IdpMetadataValidationResponse validate(IdentityProvider identityProvider);

    SamlConfigMetadata getConfigMetadata(Tenant tenant);
}
