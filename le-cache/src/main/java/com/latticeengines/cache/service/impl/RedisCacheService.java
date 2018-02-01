package com.latticeengines.cache.service.impl;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cache.CacheType;

@Component("redisCacheService")
public class RedisCacheService extends CacheServiceBase {

    private static final Logger log = LoggerFactory.getLogger(RedisCacheService.class);

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    protected RedisCacheService() {
        super(CacheType.Redis);
    }

    @Override
    public void refreshKeysByPattern(String pattern, CacheName... cacheNames) {
        redisTemplate.execute(new SessionCallback<Long>() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public Long execute(RedisOperations operations) throws DataAccessException {
                List<Object> rawkeys = redisTemplate.executePipelined(new RedisCallback<Object>() {
                    @Override
                    public Object doInRedis(RedisConnection connection) throws DataAccessException {
                        for (CacheName cacheName : cacheNames) {
                            log.info("Getting keys via pattern " + pattern + " from " + cacheName);
                            connection.keys(String.format("*%s*", pattern).getBytes());
                        }
                        return null;
                    }
                }, new StringRedisSerializer());
                Set<String> keys = rawkeys.stream().flatMap(o -> ((Set<String>) o).stream())
                        .collect(Collectors.toSet());
                long cnt = operations.delete(keys);
                log.info("Deleted " + cnt);
                return cnt;
            }
        });
    }
}
