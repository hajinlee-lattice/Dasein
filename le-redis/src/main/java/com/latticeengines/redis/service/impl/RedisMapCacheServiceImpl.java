package com.latticeengines.redis.service.impl;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
	public Set<Object> getKeys(String mapName) {
		return getMapCache(mapName).readAllKeySet();
	}

	@Override
	public Future<Set<Object>> getKeysAsync(String mapName) {
		return getMapCache(mapName).readAllKeySetAsync();
	}

	@Override
	public Set<Object> getKeys(String mapName, String pattern) {
		return getMapCache(mapName).readAllKeySet().stream().filter(k -> k.toString().matches(pattern))
				.collect(Collectors.toSet());
	}

	@Override
	public boolean deleteAllKeys(String mapName) {
		return getMapCache(mapName).delete();
	}

	@Override
	public Future<Boolean> deleteAllKeysAsync(String mapName) {
		return getMapCache(mapName).deleteAsync();
	}

	@Override
	public Long deleteKeys(String mapName, Object... keys) {
		return getMapCache(mapName).fastRemove(keys);
	}

	@Override
	public Future<Long> deleteKeysAsync(String mapName, Object... keys) {
		return getMapCache(mapName).fastRemoveAsync(keys);
	}

	@Override
	public Long deleteKeysByPattern(String mapName, String pattern) {
		Set<Object> keys = getKeys(mapName, pattern);
		return deleteKeys(mapName, keys.toArray());
	}

	@Override
	public Future<Long> deleteKeysByPatternAsync(String mapName, String pattern) {
		Set<Object> keys = getKeys(mapName, pattern);
		return deleteKeysAsync(mapName, keys.toArray());
	}

	@Override
	public Object getValue(String mapName, Object key) {
		return getMapCache(mapName).get(key);
	}

	@Override
	public Future<Object> getValueAsync(String mapName, Object key) {
		return getMapCache(mapName).getAsync(key);
	}

	@Override
	public Map<Object, Object> getValues(String mapName, Set<Object> keys) {
		return getMapCache(mapName).getAll(keys);
	}

	@Override
	public Future<Map<Object, Object>> getValuesAsync(String mapName, Set<Object> keys) {
		return getMapCache(mapName).getAllAsync(keys);
	}

	@Override
	public boolean fastPutValue(String mapName, Object key, Object value) {
		return getMapCache(mapName).fastPut(key, value);
	}
}
