package com.latticeengines.redis.exposed.service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

public interface RedisService {

    Set<Object> getKeys(String cacheName);

    Future<Set<Object>> getKeysAsync(String mapName);

    Set<Object> getKeys(String cacheName, String pattern);

    boolean deleteAllKeys(String cacheName);

    Future<Boolean> deleteAllKeysAsync(String mapName);

    Long deleteKeys(String cacheName, Object... keys);

    Future<Long> deleteKeysAsync(String mapName, Object... keys);

    Long deleteKeysByPattern(String cacheName, String pattern);

    Future<Long> deleteKeysByPatternAsync(String mapName, String pattern);

    Object getValue(String mapName, Object key);

    Future<Object> getValueAsync(String mapName, Object key);

    Object getValues(String mapName, Set<Object> keys);

    Future<Map<Object, Object>> getValuesAsync(String mapName, Set<Object> keys);

    boolean fastPutValue(String mapName, Object key, Object value);

}
