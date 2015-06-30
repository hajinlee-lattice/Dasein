package com.latticeengines.oauth2.common.service.impl;

import org.joda.time.DateTime;
import org.springframework.security.oauth2.provider.client.BaseClientDetails;

import com.latticeengines.oauth2.common.service.ExtendedClientDetails;

public class OAuthClientDetails extends BaseClientDetails implements ExtendedClientDetails {

    public OAuthClientDetails() {
    }

    public OAuthClientDetails(BaseClientDetails base) {
        setClientId(base.getClientId());
        setClientSecret(base.getClientSecret());
        setScope(base.getScope());
        setResourceIds(base.getResourceIds());
        setAuthorizedGrantTypes(base.getAuthorizedGrantTypes());
        setRegisteredRedirectUri(base.getRegisteredRedirectUri());
        setAutoApproveScopes(base.getAutoApproveScopes());
        setAuthorities(base.getAuthorities());
        setAccessTokenValiditySeconds(base.getAccessTokenValiditySeconds());
        setRefreshTokenValiditySeconds(base.getRefreshTokenValiditySeconds());
        setAdditionalInformation(base.getAdditionalInformation());
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @org.codehaus.jackson.annotate.JsonIgnore
    @com.fasterxml.jackson.annotation.JsonIgnore
    @Override
    public DateTime getClientSecretExpiration() {
        return clientSecretExpiration;
    }

    public void setClientSecretExpiration(DateTime stamp) {
        clientSecretExpiration = stamp;
    }

    @org.codehaus.jackson.annotate.JsonProperty("client_secret_expiration")
    @com.fasterxml.jackson.annotation.JsonProperty("client_secret_expiration")
    private DateTime clientSecretExpiration;
}
