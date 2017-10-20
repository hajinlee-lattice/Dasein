package com.latticeengines.cache.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.redis.exposed.service.RedisService;

@Component("cacheService")
public class CacheServiceImpl implements CacheService {

    @Autowired
    private RedisService redisService;

    @Override
    public void dropKeysByPattern(String cacheName, String pattern) {
        redisService.deleteKeysByPattern(cacheName, pattern);
    }

}
