package com.latticeengines.proxy.exposed.oauth2;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("oauth2RestApiProxy")
public class Oauth2RestApiProxy extends BaseRestApiProxy implements Oauth2Interface {

    private static final String CLIENT_ID_LP = OauthClientType.LP.getValue();

    @Value("${common.oauth.url}")
    protected String oauth2AuthHostPort;

    protected OAuth2RestTemplate oAuth2RestTemplate = null;

    public Oauth2RestApiProxy() {
        super(PropertyUtils.getProperty("common.playmaker.url"));
    }

    @Override
    public String createAPIToken(String tenantId) {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setTenantName(tenantId);
        tenant.setJdbcDriver("dummy");
        tenant.setJdbcUrl("dummy");
        String url = constructUrl("/tenants");
        PlaymakerTenant outputTenant = post("create-api-token", url, tenant, PlaymakerTenant.class);
        if (outputTenant == null || StringUtils.isEmpty(outputTenant.getTenantPassword())) {
            throw new RuntimeException("Failed to get api password from playmaker api");
        }
        return outputTenant.getTenantPassword();
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId) {
        String apiToken = createAPIToken(tenantId);
        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(oauth2AuthHostPort, tenantId, apiToken, CLIENT_ID_LP, appId);
        return OAuth2Utils.getAccessToken(oAuth2RestTemplate);
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId, OauthClientType type) {
        String apiToken = createAPIToken(tenantId);
        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(oauth2AuthHostPort, tenantId, apiToken, type.getValue(),
                appId);
        return OAuth2Utils.getAccessToken(oAuth2RestTemplate);
    }
}
