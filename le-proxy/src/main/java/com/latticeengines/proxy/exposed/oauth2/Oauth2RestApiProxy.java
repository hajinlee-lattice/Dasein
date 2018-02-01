package com.latticeengines.proxy.exposed.oauth2;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.RequestEntity;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("oauth2RestApiProxy")
public class Oauth2RestApiProxy extends BaseRestApiProxy implements Oauth2Interface {

    @Value("${common.oauth.url}")
    protected String oauth2AuthHostPort;

    protected ThreadLocal<OAuth2RestTemplate> oAuth2RestTemplate = new ThreadLocal<>();

    public Oauth2RestApiProxy() {
        super(PropertyUtils.getProperty("common.microservice.url"), "/lp");
    }

    @Override
    public String createAPIToken(String userId) {
        String url = constructUrl("/oauthotps?user={userId}", userId);
        ResponseDocument responseDocument = get("generate-otp", url, ResponseDocument.class);
        if (!responseDocument.isSuccess()) {
            throw new RuntimeException("Failed to generate one time password for userId=" + userId //
                    + ":" + StringUtils.join(responseDocument.getErrors(), ","));
        } else {
            return (String) responseDocument.getResult();
        }
    }
    
    public PlaymakerTenant createOAuthTenant(PlaymakerTenant playmakerTenant) {
        String url = constructUrl("/playmaker/tenants");
        return post("Create Tenant", url, playmakerTenant, PlaymakerTenant.class);
    }
    
    public PlaymakerTenant getOAuthTenant(String tenantName) {
        String url = constructUrl("/playmaker/tenants/{tenantName}", tenantName);
        return get("GetTenant", url, PlaymakerTenant.class);
    }
    
    public String getTenantNameFromOAuthRequest(RequestEntity<String> requestEntity) {
        String url = constructUrl("/playmaker/tenants/oauthtotenant");
        return get("GetTenantName", url, requestEntity, String.class);
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId) {
        return createOAuth2AccessToken(tenantId, appId, OauthClientType.LP);
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId, OauthClientType type) {
        String apiToken = createAPIToken(tenantId);
        oAuth2RestTemplate.set(OAuth2Utils.getOauthTemplate(oauth2AuthHostPort, tenantId, apiToken, type.getValue(),
                appId));
        return OAuth2Utils.getAccessToken(oAuth2RestTemplate.get());
    }
}
