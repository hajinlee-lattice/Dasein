package com.latticeengines.remote.service.impl;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.proxy.exposed.RestApiClient;
import com.latticeengines.remote.exposed.service.IDaaSService;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;

@Service
public class IDaaSServiceImpl implements IDaaSService  {

    @Inject
    private ApplicationContext appCtx;

    @Value("${security.idaas.api.url}")
    private String apiUrl;

    @Value("${security.idaas.client.id}")
    private String clientId;

    @Value("${security.idaas.client.secret.encrypted}")
    private String clientSecret;

    private volatile RestApiClient client;
    private volatile String tokenInUse;
    private final LoadingCache<String, String> tokenCache = Caffeine.newBuilder() //
            .maximumSize(1000) //
            .expireAfterWrite(3, TimeUnit.HOURS) //
            .build(this::refreshOAuthTokens);

    @Override
    public String getEntitlement(String subscriberNumber) {
        refreshToken();
        return getClient().get(null, entitlementUri(subscriberNumber));
    }

    private RestApiClient getClient() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = RestApiClient.newExternalClient(appCtx);
                    client.setHostport(apiUrl);
                    client.setUseUri(true);
                }
            }
        }
        return client;
    }

    private String refreshOAuthTokens(String cacheKey) {
        Map<String, String> payload = ImmutableMap.of("grant_type", "client_credentials");
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        String headerValue = String.format("client_id:%s,client_secret:%s", clientId, clientSecret);
        ClientHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(headerValue);
        restTemplate.setInterceptors(Collections.singletonList(interceptor));
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(3);
        JsonNode jsonNode = retryTemplate.execute(ctx -> {
            try(PerformanceTimer timer = new PerformanceTimer("Get OAuth2 token from IDaaS.")) {
                ResponseEntity<JsonNode> response = restTemplate.postForEntity(oauthTokenUri(), payload, JsonNode.class);
                JsonNode body = response.getBody();
                if (body != null) {
                    return body;
                } else {
                    throw new RuntimeException("Get empty response from oauth request.");
                }
            }
        });
        return jsonNode.get("access_token").asText();
    }

    private void refreshToken() {
        String token = tokenCache.get("token");
        Preconditions.checkNotNull(token, "oauth token cannot be null");
        if (!token.equals(tokenInUse)) {
            synchronized (this) {
                if (!token.equals(tokenInUse)) {
                    tokenInUse = token;
                    String headerValue = "Bearer " + tokenInUse;
                    getClient().setAuthHeader(headerValue);
                }
            }
        }
    }

    private String oauthTokenUri() {
        return URI.create(apiUrl + "/oauth2/v3/token").toString();
    }

    private String entitlementUri(String subscriberNumber) {
        return URI.create(apiUrl + "/entitlement/v1/subscriber/" + subscriberNumber).toString();
    }

}
