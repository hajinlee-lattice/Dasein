package com.latticeengines.redis.exposed.service;

import java.util.concurrent.TimeUnit;

public interface RedisMapCacheService extends RedisMapService {

    boolean fastPutValue(String mapName, Object key, Object value, long ttl, TimeUnit ttlUnit);

    boolean fastPut(String mapName, Object key, Object value, long ttl, TimeUnit ttlUnit, long maxIdleTime,
            TimeUnit maxIdleUnit);
}
