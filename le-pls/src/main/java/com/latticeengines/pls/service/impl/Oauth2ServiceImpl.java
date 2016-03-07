package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;
import com.latticeengines.proxy.exposed.oauth.Oauth2RestApiProxy;

@Component("oauth2Service")
public class Oauth2ServiceImpl implements Oauth2Interface {

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;
 
    @Autowired
    private Oauth2AccessTokenEntityMgr oauth2AccessTokenEntityMgr;

    @Override
    public String createAPIToken(String tenantId) {
        return oauth2RestApiProxy.createAPIToken(tenantId);
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId) {
        OAuth2AccessToken oAuth2AccessToken = oauth2RestApiProxy.createOAuth2AccessToken(tenantId);
        Oauth2AccessToken oauth2AccessToken = new Oauth2AccessToken();
        oauth2AccessToken.setAccessToken(oAuth2AccessToken.getValue());
        oauth2AccessTokenEntityMgr.createOrUpdate(oauth2AccessToken);
        return oAuth2AccessToken;
    }

}
