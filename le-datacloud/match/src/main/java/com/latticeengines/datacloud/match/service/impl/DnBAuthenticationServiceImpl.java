package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.kitesdk.shaded.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.jayway.jsonpath.JsonPath;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.RestApiClient;

@Component("dnbAuthenticationService")
public class DnBAuthenticationServiceImpl implements DnBAuthenticationService {
    private static final Logger log = LoggerFactory.getLogger(DnBAuthenticationServiceImpl.class);

    @Value("${datacloud.dnb.realtime.api.key}")
    private String realtimeKey;

    @Value("${datacloud.dnb.bulk.api.key}")
    private String batchKey;

    @Value("${datacloud.dnb.realtime.password.encrypted}")
    private String realtimePwd;

    @Value("${datacloud.dnb.bulk.password.encrypted}")
    private String batchPwd;

    @Value("${datacloud.dnb.user.header}")
    private String username;

    @Value("${datacloud.dnb.password.header}")
    private String pwd;

    @Value("${datacloud.dnb.authority.url}")
    private String url;

    @Value("${datacloud.dnb.application.id.header}")
    private String appIdHeader;

    @Value("${datacloud.dnb.application.id}")
    private String appId;

    @Value("${datacloud.dnb.token.cache.expiration.duration.minute}")
    private int expireTimeInMin;

    @Value("${datacloud.dnb.authentication.token.jsonpath}")
    private String tokenJsonPath;

    // Local cache doesn't refresh automatically
    // There are 2 cases that tokenCache might be refreshed:
    // 1. In initialization, load token from Redis
    // 2. External service found current cached token has been expired
    private LoadingCache<DnBKeyType, String> tokenCache;

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    private RestApiClient dnbClient;

    @PostConstruct
    public void initialize() throws Exception {
        dnbClient = RestApiClient.newExternalClient(applicationContext);
        tokenCache = //
                CacheBuilder.newBuilder()//
                        .maximumSize(DnBKeyType.values().length)
                        .build(new CacheLoader<DnBKeyType, String>() {
                            @Override
                            public String load(DnBKeyType type) throws Exception {
                                return externalRequest(type, null);
                            }
                        });
    }


    @Override
    public String requestToken(@NotNull DnBKeyType type, String expiredToken) {
        Preconditions.checkNotNull(type);
        String localToken = localRequest(type);
        // Handles the case that expiredToken is not provided or
        // localToken has been refreshed
        if (!localToken.equals(expiredToken)) {
            return localToken;
        }
        String newToken = externalRequest(type, expiredToken);
        tokenCache.put(type, newToken);
        return newToken;
    }

    /**
     * Request DnB token from local cache
     * 
     * @param type:
     *            DnB key type -- realtime/batch
     * @return localCache
     */
    private String localRequest(@NotNull DnBKeyType type) {
        try {
            return tokenCache.get(type);
        } catch (ExecutionException e) {
            log.error("Fail to get DnB " + type + " token from local cache", e);
            throw new LedpException(LedpCode.LEDP_25027, e);
        }
    }

    /**
     * Request DnB token externally from Redis/DnB
     *
     * When to use token cached in Redis (all the conditions should be
     * satisfied): 1. Cached token is not empty; 2. Cached token was created
     * within {expireTimeInMin} minutes 3. Cached token is different with
     * {expiredToken}
     * 
     * @param type
     * @param expiredToken
     * @return
     */
    private synchronized String externalRequest(@NotNull DnBKeyType type, String expiredToken) {
        String redisToken = redisRequest(type);
        if (!redisToken.equals(expiredToken)) {
            return redisToken;
        }

        return null;
    }

    /**
     * Request DnB token from Redis with some validations in case Redis data is
     * illegally updated outside of application
     *
     * @param type:
     *            DnB key type -- realtime/batch
     * @return token
     */
    private String redisRequest(DnBKeyType type) {
        try {
            Object obj = redisTemplate.opsForValue().get(type.name());
            // pair of (token, timestamp)
            @SuppressWarnings("unchecked")
            Pair<String, Long> pair = (obj instanceof Pair) ? (Pair<String, Long>) obj : null;
            if (pair != null) {
                pair = StringUtils.isNotBlank(pair.getLeft()) && pair.getRight() != null ? pair : null;
            }
            if (pair != null && !isTimestampExpired(pair.getRight())) {
                return pair.getLeft();
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("Fail to get DnB " + type + " token from redis cache", e);
            return null;
        }
    }

    private String fetchFromDnB(DnBKeyType type) {
        String token = "";
        try {
            String response = makeDnBRequest(type);
            token = parseResponse(response);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        log.info("Refreshed DnB token to be {}", token);

        return token;
    }

    private boolean isTimestampExpired(long timestamp) {
        return (System.currentTimeMillis() - timestamp) > expireTimeInMin * 60_000;
    }

    /**
     * Make DnB API call and return raw response
     *
     * @param type:
     *            DnB key type
     * @return response
     * @throws IOException
     */
    private String makeDnBRequest(DnBKeyType type) throws IOException {
        switch (type) {
        case REALTIME:
            return dnbClient.post(getHttpEntity(realtimeKey, realtimePwd), url);
        case BATCH:
            return dnbClient.post(getHttpEntity(batchKey, batchPwd), url);
        default:
            throw new UnsupportedOperationException(
                    String.format("DnBKeyType %s is not supported in DnBAuthenticationService.", type.name()));
        }
    }

    private HttpEntity<String> getHttpEntity(String apiKey, String passwd) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(username, apiKey);
        headers.add(pwd, passwd);
        return new HttpEntity<>("", headers);
    }

    /**
     * Parse token from DnB API response
     *
     * @param response:
     *            DnB API response
     * @return token
     */
    private String parseResponse(String response) {
        String token = JsonPath.parse(response).read(tokenJsonPath);
        if (token == null) {
            throw new RuntimeException(String.format("Failed to parse token from response %s!", response));
        }
        return token;
    }

}
