package com.latticeengines.pls.service;

import org.springframework.security.oauth2.common.OAuth2AccessToken;

public interface OauthService {

    String createAPIToken(String tenantId);

    OAuth2AccessToken createOAuth2AccessToken(String tenantId);
}
