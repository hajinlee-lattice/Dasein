package com.latticeengines.cache.service.impl;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cache.CacheType;

@Component("redisCacheService")
public class RedisCacheService extends CacheServiceBase {

    private static final Logger log = LoggerFactory.getLogger(RedisCacheService.class);

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    @Resource(name = "localCacheService")
    private CacheService localCacheService;

    protected RedisCacheService() {
        super(CacheType.Redis);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void refreshKeysByPattern(String pattern, CacheName... cacheNames) {
        RetryTemplate retry = getRetryTemplate();
        retry.execute(context -> {
            redisTemplate.execute((SessionCallback) operations -> {
                List<Object> rawkeys = redisTemplate.executePipelined((SessionCallback) ops -> {
                    for (CacheName cacheName : cacheNames) {
                        log.info("Getting keys via pattern " + pattern + " from " + cacheName);
                        ops.keys(String.format("*%s*", pattern));
                    }
                    return null;
                }, new StringRedisSerializer());
                Set<String> keys = rawkeys.stream().flatMap(o -> ((Set<String>) o).stream())
                        .collect(Collectors.toSet());
                long cnt = operations.delete(keys);
                log.info("Deleted " + cnt);
                return cnt;
            });
            return null;
        });
        localCacheService.refreshKeysByPattern(pattern, cacheNames);
    }

    private RetryTemplate getRetryTemplate() {
        RetryTemplate retry = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);
        retry.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000L);
        backOffPolicy.setMultiplier(2.0);
        retry.setBackOffPolicy(backOffPolicy);
        retry.setThrowLastExceptionOnExhausted(true);
        return retry;
    }
}
