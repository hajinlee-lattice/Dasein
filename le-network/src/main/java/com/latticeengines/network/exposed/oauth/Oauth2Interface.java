package com.latticeengines.network.exposed.oauth;

import org.springframework.security.oauth2.common.OAuth2AccessToken;

public interface Oauth2Interface {

    String createAPIToken(String tenantId);

    OAuth2AccessToken createOAuth2AccessToken(String tenantId);
}
