package com.latticeengines.proxy.exposed.oauth;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.security.exposed.util.BaseRestApiProxy;

@Component("oauthRestApiProxy")
public class Oauth2RestApiProxy extends BaseRestApiProxy implements Oauth2Interface {

    private static final String CLIENT_ID_LP = "lp";

    @Value("${proxy.oauth.api.rest.endpoint.hostport}")
    private String oauthApiHostPort;

    @Value("${proxy.oauth.auth.rest.endpoint.hostport}")
    protected String oauthAuthHostPort;

    protected OAuth2RestTemplate oAuth2RestTemplate = null;

    @Override
    public String getRestApiHostPort() {
        return oauthAuthHostPort;
    }

    @Override
    public String createAPIToken(String tenantId) {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setTenantName(tenantId);
        tenant.setJdbcDriver("dummy");
        tenant.setJdbcUrl("dummy");
        String url = oauthApiHostPort + "/tenants";
        tenant = restTemplate.postForObject(url, tenant, PlaymakerTenant.class);
        return tenant.getTenantPassword();
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId) {
        String apiToken = createAPIToken(tenantId);
        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(oauthAuthHostPort, tenantId, apiToken,
                CLIENT_ID_LP);
        OAuth2AccessToken token = oAuth2RestTemplate.getAccessToken();
        return token;

    }
}
