package com.latticeengines.saml;

import java.util.Collection;

import org.opensaml.xml.security.credential.Credential;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.security.saml.trust.MetadataCredentialResolver;

public class LatticeMetadataCredentialResolver extends MetadataCredentialResolver {
    public LatticeMetadataCredentialResolver(MetadataManager metadataProvider, KeyManager keyManager) {
        super(metadataProvider, keyManager);
    }

    @Override
    protected void cacheCredentials(MetadataCredentialResolver.MetadataCacheKey cacheKey,
            Collection<Credential> credentials) {
        // Override so that metadata resolver doesn't cache
    }
}
