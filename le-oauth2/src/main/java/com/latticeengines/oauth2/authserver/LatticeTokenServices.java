package com.latticeengines.oauth2.authserver;

import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.DefaultTokenServices;
import org.springframework.security.oauth2.provider.token.TokenStore;

public class LatticeTokenServices extends DefaultTokenServices {

    private TokenStore tokenStore;

    public LatticeTokenServices(TokenStore tokenStore) {
        this.tokenStore = tokenStore;
    }

    @Override
    public OAuth2AccessToken createAccessToken(OAuth2Authentication authentication) throws AuthenticationException {
        OAuth2AccessToken existingAccessToken = tokenStore.getAccessToken(authentication);
        if (existingAccessToken != null) {
            revokeToken(existingAccessToken.getValue());
        }
        return super.createAccessToken(authentication);
    }
}
