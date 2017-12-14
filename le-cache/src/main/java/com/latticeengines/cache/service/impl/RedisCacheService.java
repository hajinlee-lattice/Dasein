package com.latticeengines.cache.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cache.CacheType;
import com.latticeengines.redis.exposed.service.RedisMapCacheService;

@Component("redisCacheService")
public class RedisCacheService extends CacheServiceBase {

    private static final Logger log = LoggerFactory.getLogger(RedisCacheService.class);

    @Inject
    private RedisMapCacheService redisMapCacheService;

    protected RedisCacheService() {
        super(CacheType.Redis);
    }

    @Override
    public void refreshKeysByPattern(String pattern, CacheName... cacheNames) {
        for (CacheName cacheName : cacheNames) {
            log.info("Deleting key pattern " + pattern + " from " + cacheName);
            redisMapCacheService.deleteKeysByPattern(cacheName.name(), String.format("^%s.*", pattern));
        }
    }
}
