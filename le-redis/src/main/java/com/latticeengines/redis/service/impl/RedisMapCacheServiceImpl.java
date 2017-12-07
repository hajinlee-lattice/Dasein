package com.latticeengines.redis.service.impl;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import com.latticeengines.redis.exposed.service.RedisMapCacheService;

@Component("redisMapCacheService")
public class RedisMapCacheServiceImpl extends RedisMapServiceImpl implements RedisMapCacheService {

	@Inject
	private RedissonClient redisson;

	private RMapCache<Object, Object> getMapCache(String mapName) {
		return redisson.getMapCache(mapName);
	}

	@Override
	public boolean fastPutValue(String mapName, Object key, Object value, long ttl, TimeUnit ttlUnit) {
		return getMapCache(mapName).fastPut(key, value, ttl, ttlUnit);
	}

	@Override
	public boolean fastPut(String mapName, Object key, Object value, long ttl, TimeUnit ttlUnit, long maxIdleTime,
			TimeUnit maxIdleUnit) {
		return getMapCache(mapName).fastPut(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit);
	}

	@Override
	public Long deleteKeys(String mapName, Object... keys) {
		return getMapCache(mapName).fastRemove(keys);
	}

	@Override
	public Future<Long> deleteKeysAsync(String mapName, Object... keys) {
		return getMapCache(mapName).fastRemoveAsync(keys);
	}
}
