package com.latticeengines.pls.service.impl;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;
import com.latticeengines.proxy.exposed.oauth2.LatticeOAuth2RestTemplateFactory;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

@Component("oauth2Service")
public class Oauth2ServiceImpl implements Oauth2Interface {

    private static final String CLIENT_ID_LP = "lp";

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Autowired
    private Oauth2AccessTokenEntityMgr oauth2AccessTokenEntityMgr;

    @Autowired
    protected LatticeOAuth2RestTemplateFactory latticeOAuth2RestTemplateFactory;

    @Value("${common.playmaker.url}")
    protected String playmakerUrl;

    @Override
    public String createAPIToken(String tenantId) {
        return oauth2RestApiProxy.createAPIToken(tenantId);
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId) {
        if (StringUtils.isEmpty(appId)) {
            // use null if appId is empty
            appId = null;
        }

        OAuthUser user = new OAuthUser();
        user.setUserId(tenantId);
        user.setPassword(oauth2RestApiProxy.createAPIToken(tenantId));

        OAuth2RestTemplate oAuth2RestTemplate = null;

        if (StringUtils.isEmpty(appId)) {
            oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(playmakerUrl, user.getUserId(),
                    user.getPassword(), CLIENT_ID_LP);
        } else {
            oAuth2RestTemplate = latticeOAuth2RestTemplateFactory.getOAuth2RestTemplate(user, CLIENT_ID_LP, appId);
        }

        OAuth2AccessToken oAuth2AccessToken = oAuth2RestTemplate.getAccessToken();
        Oauth2AccessToken oauth2AccessToken = new Oauth2AccessToken();
        oauth2AccessToken.setAccessToken(oAuth2AccessToken.getValue());
        oauth2AccessTokenEntityMgr.createOrUpdate(oauth2AccessToken, tenantId, appId);
        return oAuth2AccessToken;
    }
}
