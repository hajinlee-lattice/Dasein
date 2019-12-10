package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.DnBToken;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jayway.jsonpath.JsonPath;
import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.RestApiClient;
import com.latticeengines.redis.lock.RedisDistributedLock;

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

    // A DnB token expires after 24 hours. We treat it as expired after 20 hours
    // (with some time buffer) and request a new one from DnB
    @Value("${datacloud.dnb.token.cache.expiration.duration.minute}")
    private int expireTimeInMin;

    // Local cache tries to sync up token from redis every hour
    @Value("${datacloud.dnb.token.cache.reload.duration.minute}")
    private int reloadTimeInMin;

    @Value("${datacloud.dnb.authentication.token.jsonpath}")
    private String tokenJsonPath;

    private WatcherCache<DnBKeyType, String> tokenCache;

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    private RestApiClient dnbClient;

    @Inject
    private RedisDistributedLock redisLock;
    
    static final String DNB_KEY_PREFIX = "DnBAPIKey_";
    static final String DNB_LOCK_PREFIX = "DnBLockKey_";

    @SuppressWarnings("unchecked")
    @PostConstruct
    public void initialize() throws Exception {
        dnbClient = RestApiClient.newExternalClient(applicationContext);
        tokenCache = WatcherCache.builder() //
                .name("DnBTokenCache") //
                .watch(DnBToken.name()) //
                .maximum(DnBKeyType.values().length) //
                .load(type -> externalRequest((DnBKeyType) type, null)) //
                // There could be hundreds of yarn containers running at same
                // time. Add some random waiting time to avoid redis connection
                // congestion
                .waitBeforeRefreshInSec(
                        BeanFactoryEnvironment.Environment.AppMaster.equals(BeanFactoryEnvironment.getEnvironment())
                                ? new Random().nextInt(15)
                                : 0)
                .build();
        // local cache tries to sync up token from redis periodically
        tokenCache.setExpire(reloadTimeInMin, TimeUnit.MINUTES);
    }


    @Override
    public String requestToken(@NotNull DnBKeyType type, String expiredToken) {
        Preconditions.checkNotNull(type);
        String localToken = localRequest(type);
        // Handles the case that expiredToken is not provided or
        // localToken has been refreshed
        if (localToken != null && !localToken.equals(expiredToken)) {
            return localToken;
        }
        String newToken = externalRequest(type, expiredToken);
        if (newToken == null) {
            throw new LedpException(LedpCode.LEDP_25027);
        }
        tokenCache.put(type, newToken);
        return newToken;
    }

    @Override
    public String refreshToken(@NotNull DnBKeyType type, String newToken) {
        if (newToken == null) {
            newToken = dnbRequest(type);
        }
        if (newToken == null) {
            throw new RuntimeException(
                    "Fail to request a " + type + " token from DnB. Please provide expected token.");
        }

        // Acquire the lock to refresh token in redis
        String lockKey = DNB_LOCK_PREFIX + type;
        String reqId = UUID.randomUUID().toString();
        boolean acquired = false;
        int attempt = 0;
        // Every attempt to fetch lock takes up to 5s. Try for up to 6 times so
        // that total wait time is 30s.
        while (attempt < 6) {
            acquired = redisLock.lock(lockKey, reqId, 60000, true);
            if (acquired) {
                break;
            }
            attempt++;
        }
        if (!acquired) {
            throw new RuntimeException("Fail to acquire lock in redis to refresh DnB token.");
        }
        redisRefresh(type, newToken);
        redisLock.releaseLock(lockKey, reqId);

        // trigger watcher cache refresh signal so that all the tomcat apps &
        // yarn containers could reload token from redis to local cache
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        NodeWatcher.updateWatchedData(DnBToken.name(), df.format(new Date()));

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
        return tokenCache.get(type);
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
        // Fetch token from local cache again in case other threads already
        // refreshed local cache when current thread is blocked by synchronized
        // externalRequest()
        if (expiredToken != null) {
            String localToken = localRequest(type);
            if (localToken != null && !localToken.equals(expiredToken)) {
                return localToken;
            }
        }

        String redisToken = redisRequest(type, false);
        if (redisToken != null && !redisToken.equals(expiredToken)) {
            return redisToken;
        }

        // Acquire the lock and go to remote DnB to refresh token
        String lockKey = DNB_LOCK_PREFIX + type;
        String reqId = UUID.randomUUID().toString();
        boolean acquired = false;
        int attempt = 0;
        // Every attempt to fetch lock takes up to 5s. Try for up to 6 times so
        // that total wait time is 30s.
        while (attempt < 6) {
            acquired = redisLock.lock(lockKey, reqId, 60000, true);
            if (acquired) {
                break;
            }
            // Fetch token from redis again in case other applications already
            // refreshed redis cache
            redisToken = redisRequest(type, false);
            if (redisToken != null && !redisToken.equals(expiredToken)) {
                return redisToken;
            }
            attempt++;
        }

        if (!acquired) {
            return null;
        }
        // Fetch token from redis again in case other applications already
        // refreshed redis cache
        redisToken = redisRequest(type, true);
        if (redisToken != null && !redisToken.equals(expiredToken)) {
            redisLock.releaseLock(lockKey, reqId);
            return redisToken;
        }

        String dnbToken = dnbRequest(type);
        if (dnbToken != null) {
            redisRefresh(type, dnbToken);
        }
        redisLock.releaseLock(lockKey, reqId);
        return dnbToken;
    }

    /**
     * Request DnB token from Redis
     *
     * @param type:
     *            DnB key type -- realtime/batch
     * @param logIfExpire:
     *            when redis cache is expired, only log when this flag is true
     * @return token
     */
    private String redisRequest(DnBKeyType type, boolean logIfExpire) {
        try {
            DnBTokenCache cache = (DnBTokenCache) redisTemplate.opsForValue().get(DNB_KEY_PREFIX + type);
            if (cache == null) {
                log.info("There is no DnB token cached in redis, updating token from dnb.");
                return null;
            }
            log.info("Got DnB token from redis: {}", JsonUtils.serialize(cache));
            if (!isTimestampExpired(cache, logIfExpire)) {
                return cache.getToken();
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("Fail to get DnB " + type + " token from redis cache", e);
            return null;
        }
    }

    /**
     * Refresh DnB token cached in redis
     *
     * @param type:
     *            DnB key type -- realtime/batch
     * @param token
     */
    private void redisRefresh(DnBKeyType type, String token) {
        redisTemplate.opsForValue().set(DNB_KEY_PREFIX + type, new DnBTokenCache(token, System.currentTimeMillis()));
    }

    /**
     * Whether cached token has approaching expiration in timestamp
     *
     * @param timestamp:
     *            create timestamp of token
     * @param logIfExpire:
     *            when redis cache is expired, only log when this flag is true
     * @return
     */
    private boolean isTimestampExpired(DnBTokenCache cache, boolean logIfExpire) {
        boolean expired = (System.currentTimeMillis() - cache.getCreatedAt()) > expireTimeInMin * 60_000;
        if (expired && logIfExpire) {
            log.info("DnB token in redis {} has existed for more than {}hrs, updating token from dnb.",
                    cache.getToken(), expireTimeInMin / 60);
        }
        return expired;
    }

    /**
     * Request DnB token from DnB authentication API
     *
     * @param type:
     *            DnB key type -- realtime/batch
     * @return
     */
    private String dnbRequest(DnBKeyType type) {
        try {
            String response = dnbAuthenticateRequest(type);
            String token = parseDnBResponse(response);
            log.info("Get new DnB " + type + " token {}", token);
            return token;
        } catch (Exception e) {
            log.error("Fail to get DnB " + type + " token from remote DnB API", e);
            return null;
        }
    }

    /**
     * Make DnB authentication API call
     *
     * @param type:
     *            DnB key type -- realtime/batch
     * @return response: DnB authentication API response
     * @throws IOException
     */
    private String dnbAuthenticateRequest(DnBKeyType type) throws IOException {
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
    private String parseDnBResponse(String response) {
        String token = JsonPath.parse(response).read(tokenJsonPath);
        if (StringUtils.isBlank(token)) {
            throw new RuntimeException(String.format("Fail to parse DnB token from response: %s", response));
        }
        return token;
    }
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class DnBTokenCache {
        @JsonProperty("Token")
        private String token;
        @JsonProperty("CreatedAt")
        private long createdAt;
        @JsonProperty("CreatedBy")
        private String createdBy;

        @JsonCreator
        DnBTokenCache(@JsonProperty("Token") String token, @JsonProperty("CreatedAt") long createdAt) {
            this.token = token;
            this.createdAt = createdAt;
            this.createdBy = BeanFactoryEnvironment.getEnvironment().toString();
        }

        String getToken() {
            return token;
        }

        long getCreatedAt() {
            return createdAt;
        }
    }
}
