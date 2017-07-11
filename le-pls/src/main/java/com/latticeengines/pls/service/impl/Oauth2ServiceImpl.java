package com.latticeengines.pls.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.DefaultOAuth2AccessToken;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;
import com.latticeengines.proxy.exposed.oauth2.LatticeOAuth2RestTemplateFactory;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

@Component("oauth2Service")
public class Oauth2ServiceImpl implements Oauth2Interface {

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Autowired
    private Oauth2AccessTokenEntityMgr oauth2AccessTokenEntityMgr;

    @Autowired
    protected LatticeOAuth2RestTemplateFactory latticeOAuth2RestTemplateFactory;

    @Value("${common.oauth.url}")
    protected String oauth2Url;

    @Override
    public String createAPIToken(String tenantId) {
        return oauth2RestApiProxy.createAPIToken(tenantId);
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId) {
        return createOAuth2AccessToken(tenantId, appId, OauthClientType.LP);
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId, OauthClientType type) {
        if (StringUtils.isEmpty(appId)) {
            // use null if appId is empty
            appId = null;
        }

        Oauth2AccessToken token = oauth2AccessTokenEntityMgr.get(tenantId, appId);
        long now = System.currentTimeMillis();
        if(token.getLastModifiedTime() + 10 *1000 > now && StringUtils.isNotEmpty(token.getAccessToken())) {
            return new DefaultOAuth2AccessToken(token.getAccessToken());
        }
        else {
            OAuthUser user = new OAuthUser();
            user.setUserId(tenantId);
            user.setPassword(oauth2RestApiProxy.createAPIToken(tenantId));

            OAuth2RestTemplate oAuth2RestTemplate;

            if (StringUtils.isEmpty(appId)) {
                oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(oauth2Url, user.getUserId(), user.getPassword(),
                        type.getValue());
            } else {
                oAuth2RestTemplate = latticeOAuth2RestTemplateFactory.getOAuth2RestTemplate(user, type.getValue(), appId);
            }
            OAuth2AccessToken token1 = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
            token.setAccessToken(CipherUtils.encrypt(token1.getValue()));
            token.setLastModifiedTime(System.currentTimeMillis());
            oauth2AccessTokenEntityMgr.createOrUpdate(token);
            return token1;
        }
    }
}
