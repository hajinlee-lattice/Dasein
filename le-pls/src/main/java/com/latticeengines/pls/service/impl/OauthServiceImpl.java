package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.stereotype.Component;
import com.latticeengines.pls.service.OauthService;
import com.latticeengines.pls.util.OauthRestApiProxy;

@Component("oauthService")
public class OauthServiceImpl implements OauthService {

    @Autowired
    private OauthRestApiProxy oauthRestApiProxy;

    @Override
    public String createAPIToken(String tenantId) {
        return oauthRestApiProxy.createAPIToken(tenantId);
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId) {
        return oauthRestApiProxy.createOAuth2AccessToken(tenantId);
    }

}
