package com.latticeengines.oauth2db.exposed.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.TokenRequest;
import org.springframework.security.oauth2.provider.token.DefaultTokenServices;
import org.springframework.security.oauth2.provider.token.TokenStore;

public class LatticeTokenServices extends DefaultTokenServices {
    private static final Logger log = LoggerFactory.getLogger(LatticeTokenServices.class);

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

    @Override
    public OAuth2AccessToken refreshAccessToken(String refreshTokenValue, TokenRequest tokenRequest)
            throws AuthenticationException {
        try {
            return super.refreshAccessToken(refreshTokenValue, tokenRequest);
        } catch (DuplicateKeyException e) {
            log.error("Attempt to write the same authentication_id which is a known issue in oAuth, retry");
            return super.refreshAccessToken(refreshTokenValue, tokenRequest);
        }

    }
}
