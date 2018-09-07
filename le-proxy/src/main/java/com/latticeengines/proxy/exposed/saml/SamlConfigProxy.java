package com.latticeengines.proxy.exposed.saml;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.domain.exposed.saml.IdpMetadataValidationResponse;
import com.latticeengines.domain.exposed.saml.SamlConfigMetadata;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("samlConfigProxy")
public class SamlConfigProxy extends BaseRestApiProxy {

    public SamlConfigProxy() {
        super(PropertyUtils.getProperty("common.saml.url"), "/saml/management/identityproviders");
    }

    public IdpMetadataValidationResponse validateMetadata(String tenantId, IdentityProvider identityProvider) {
        String url = constructUrl("/{tenantId}/validate", tenantId);
        return post("validateIdpMetadata", url, identityProvider, IdpMetadataValidationResponse.class);
    }

    public IdentityProvider getConfig(String tenantId) {
        String url = constructUrl("/{tenantId}", tenantId);
        List<?> configsForTenant = get("validateIdpMetadata", url, List.class);
        IdentityProvider config = null;
        if (CollectionUtils.isNotEmpty(configsForTenant)) {
            config = JsonUtils.convertList(configsForTenant, IdentityProvider.class).get(0);
        }
        return config;
    }
    
    public SamlConfigMetadata getSamlConfigMetadata(String tenantId) {
        String url = constructUrl("/{tenantId}/config-metadata", tenantId);
        SamlConfigMetadata samlConfigMetadata = get("getSamlConfigMetadata", url, SamlConfigMetadata.class);
        return samlConfigMetadata;
    }

    public IdentityProvider saveConfig(String tenantId, IdentityProvider identityProvider) {
        IdentityProvider existingConfig = getConfig(tenantId);
        if (existingConfig != null) {
            throw new LedpException(LedpCode.LEDP_33002, new String[] { existingConfig.getEntityId() });
        }

        String url = constructUrl("/{tenantId}", tenantId);
        post("SaveIdpMetadata", url, identityProvider, Void.class);
        return getConfig(tenantId);
    }

    public void deleteConfig(String tenantId, String configId) {
        String url = constructUrl("/{tenantId}/{configId}", tenantId, configId);
        delete("deleteConfig", url);
    }
}
