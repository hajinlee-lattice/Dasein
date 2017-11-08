package com.latticeengines.cache.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.domain.exposed.cache.CacheNames;
import com.latticeengines.domain.exposed.cache.CacheType;
import com.latticeengines.redis.exposed.service.RedisService;

@Component("redisCacheService")
public class RedisCacheService extends CacheServiceBase {

    protected RedisCacheService() {
        super(CacheType.Redis);
    }

    @Inject
    private RedisService redisService;

    @Override
    public void refreshKeysByPattern(String pattern, CacheNames... cacheNames) {
        for (CacheNames cacheName : cacheNames) {
            redisService.deleteKeysByPattern(cacheName.name(), String.format("*%s*", pattern));
        }
    }
}
