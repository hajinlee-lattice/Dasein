package com.latticeengines.proxy.exposed.oauth2;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.security.exposed.util.BaseRestApiProxy;

@Component("oauth2RestApiProxy")
public class Oauth2RestApiProxy extends BaseRestApiProxy implements Oauth2Interface {

    private static final String CLIENT_ID_LP = "lp";

    @Value("${common.playmaker.url}")
    private String oauth2ApiHostPort;

    @Value("${common.oauth.url}")
    protected String oauth2AuthHostPort;

    protected OAuth2RestTemplate oAuth2RestTemplate = null;

    @Override
    public String getRestApiHostPort() {
        return oauth2AuthHostPort;
    }

    @Override
    public String createAPIToken(String tenantId) {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setTenantName(tenantId);
        tenant.setJdbcDriver("dummy");
        tenant.setJdbcUrl("dummy");
        String url = oauth2ApiHostPort + "/tenants";
        tenant = restTemplate.postForObject(url, tenant, PlaymakerTenant.class);
        return tenant.getTenantPassword();
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId) {
        String apiToken = createAPIToken(tenantId);
        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(oauth2AuthHostPort, tenantId, apiToken, CLIENT_ID_LP, appId);
        OAuth2AccessToken token = oAuth2RestTemplate.getAccessToken();
        return token;

    }
}
