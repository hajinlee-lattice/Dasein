package com.latticeengines.proxy.exposed.oauth2;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.token.AccessTokenProvider;
import org.springframework.security.oauth2.client.token.AccessTokenProviderChain;
import org.springframework.security.oauth2.client.token.grant.client.ClientCredentialsAccessTokenProvider;
import org.springframework.security.oauth2.client.token.grant.code.AuthorizationCodeAccessTokenProvider;
import org.springframework.security.oauth2.client.token.grant.implicit.ImplicitAccessTokenProvider;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

@Component
public class LatticeOAuth2RestTemplateFactory {

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;

    public OAuth2RestTemplate getOAuth2RestTemplate(OAuthUser oAuthUser, String clientId, String appId) {
        String authHostPort = oauth2RestApiProxy.getRestApiHostPort();
        return getOAuth2RestTemplate(oAuthUser, clientId, appId, authHostPort);
    }

    public OAuth2RestTemplate getOAuth2RestTemplate(OAuthUser oAuthUser, String clientId, String appId,
            String authHostPort) {
        OAuth2RestTemplate oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, oAuthUser.getUserId(),
                oAuthUser.getPassword(), clientId, appId);
        AccessTokenProvider accessTokenProvider = getAccessTokenProvider();
        oAuth2RestTemplate.setAccessTokenProvider(accessTokenProvider);
        return oAuth2RestTemplate;
    }

    private AccessTokenProvider getAccessTokenProvider() {
        return new AccessTokenProviderChain(Arrays.<AccessTokenProvider> asList(
                new AuthorizationCodeAccessTokenProvider(), new ImplicitAccessTokenProvider(),
                new LatticeResourceOwnerPasswordAccessTokenProvider(), new ClientCredentialsAccessTokenProvider()));
    }
}
