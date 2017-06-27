package com.latticeengines.network.exposed.oauth;

import org.springframework.security.oauth2.common.OAuth2AccessToken;

import com.latticeengines.domain.exposed.oauth.OauthClientType;

public interface Oauth2Interface {

    String createAPIToken(String tenantId);

    OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId);

    OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId, OauthClientType type);
}
