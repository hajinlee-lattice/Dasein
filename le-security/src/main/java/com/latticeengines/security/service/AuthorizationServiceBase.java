package com.latticeengines.security.service;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;

public abstract class AuthorizationServiceBase {

    private static final Logger log = LoggerFactory.getLogger(AuthorizationServiceBase.class);

    public static final String DCP_PRODUCT = "DnB Connect";
    public static final String DCP_ROLE = "DNB_CONNECT_ACCESS";
    private static final int MAX_RETRIES = 3;

    protected final RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    protected final LoadingCache<String, String> tokenCache = Caffeine.newBuilder() //
            .maximumSize(10) //
            .expireAfterWrite(30, TimeUnit.MINUTES) //
            .build(this::refreshOAuthTokens);

    protected volatile String tokenInUse;

    @Value("${remote.idaas.api.url}")
    private String apiUrl;

    @Value("${remote.idaas.client.id}")
    protected String clientId;

    @Value("${remote.idaas.client.secret.encrypted}")
    private String clientSecret;

    protected abstract String refreshOAuthTokens(String cacheKey);

    @Cacheable(cacheNames = CacheName.Constants.IDaaSTokenCacheName, key = "T(java.lang.String).format(\"%s|idaas-token\", #clientId)", unless = "#result == null")
    public String getTokenFromIDaaS(String clientId) {
        log.info("Refreshing for " + clientId);
        Map<String, String> payload = ImmutableMap.of("grant_type", "client_credentials");
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        String headerValue = String.format("client_id:%s,client_secret:%s", clientId, clientSecret);
        ClientHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(headerValue);
        restTemplate.setInterceptors(Collections.singletonList(interceptor));
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRIES);
        JsonNode jsonNode = retryTemplate.execute(ctx -> {
            try (PerformanceTimer timer = new PerformanceTimer("Get OAuth2 token from IDaaS.")) {
                ResponseEntity<JsonNode> response = restTemplate.postForEntity(oauthTokenUri(), payload,
                        JsonNode.class);
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

    protected void refreshToken() {
        String token = tokenCache.get("token");
        Preconditions.checkNotNull(token, "oauth token cannot be null");
        if (!token.equals(tokenInUse)) {
            synchronized (this) {
                if (!token.equals(tokenInUse)) {
                    tokenInUse = token;
                    String headerValue = "Bearer " + tokenInUse;
                    ClientHttpRequestInterceptor authHeader = new AuthorizationHeaderHttpRequestInterceptor(
                            headerValue);
                    List<ClientHttpRequestInterceptor> interceptors = restTemplate.getInterceptors();
                    interceptors.removeIf(i -> i instanceof AuthorizationHeaderHttpRequestInterceptor);
                    interceptors.add(authHeader);
                    restTemplate.setInterceptors(interceptors);
                }
            }
        }
    }

    private URI oauthTokenUri() {
        return URI.create(apiUrl + "/oauth2/v3/token");
    }
}
