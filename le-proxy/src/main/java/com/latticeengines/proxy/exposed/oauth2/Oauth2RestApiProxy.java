package com.latticeengines.proxy.exposed.oauth2;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.RequestEntity;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
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
        @SuppressWarnings("rawtypes")
        ResponseDocument responseDocument = get("generate-otp", url, ResponseDocument.class);
        if (!responseDocument.isSuccess()) {
            throw new RuntimeException("Failed to generate one time password for userId=" + userId //
                    + ":" + StringUtils.join(responseDocument.getErrors(), ","));
        } else {
            return (String) responseDocument.getResult();
        }
    }

    public PlaymakerTenant createTenant(PlaymakerTenant playmakerTenant) {
        String url = constructUrl("/playmaker/tenants");
        return post("Create Tenant", url, playmakerTenant, PlaymakerTenant.class);
    }

    public void deleteTenant(String tenantName) {
        String url = constructUrl("/playmaker/tenants/{tenantName}", tenantName);
        delete("delete playmaker tenant", url);
    }

    public PlaymakerTenant updateTenant(String tenantName, PlaymakerTenant tenant) {
        String url = constructUrl("/playmaker/tenants/{tenantName}", tenantName);
        return put("update playmaker tenant", url, tenant, PlaymakerTenant.class);
    }

    public PlaymakerTenant getTenant(String tenantName) {
        String url = constructUrl("/playmaker/tenants/{tenantName}", tenantName);
        return get("GetTenant", url, PlaymakerTenant.class);
    }

    public String getTenantNameFromOAuthRequest(RequestEntity<String> requestEntity) {
        String url = constructUrl("/playmaker/tenants/oauthtotenant");
        return get("GetTenantName", url, requestEntity, String.class);
    }

    public Map<String, String> getAppIdFromOAuthRequest(RequestEntity<String> requestEntity) {
        String url = constructUrl("/playmaker/tenants/oauthtoappid");
        @SuppressWarnings("rawtypes")
        Map resObj = get("GetAppId", url, requestEntity, Map.class);
        Map<String, String> res = null;
        if (MapUtils.isNotEmpty(resObj)) {
            res = JsonUtils.convertMap(resObj, String.class, String.class);
        }
        return res;
    }

    public Map<String, String> getOrgInfoFromOAuthRequest(RequestEntity<String> requestEntity) {
        String url = constructUrl("/playmaker/tenants/oauthtoorginfo");
        @SuppressWarnings("rawtypes")
        Map resObj = get("GetOrgInfo", url, requestEntity, Map.class);
        Map<String, String> res = null;
        if (MapUtils.isNotEmpty(resObj)) {
            res = JsonUtils.convertMap(resObj, String.class, String.class);
        }
        return res;
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId) {
        return createOAuth2AccessToken(tenantId, appId, OauthClientType.LP);
    }

    @Override
    public OAuth2AccessToken createOAuth2AccessToken(String tenantId, String appId, OauthClientType type) {
        String apiToken = createAPIToken(tenantId);
        oAuth2RestTemplate
                .set(OAuth2Utils.getOauthTemplate(oauth2AuthHostPort, tenantId, apiToken, type.getValue(), appId));
        return OAuth2Utils.getAccessToken(oAuth2RestTemplate.get());
    }
}
