package com.latticeengines.redis.service.impl;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.redis.exposed.service.RedisService;

@Component("redisService")
public class RedisServiceImpl implements RedisService {

    @Autowired
    private RedissonClient redisson;

    @Override
    public Set<Object> getKeys(String mapName) {
        return getMap(mapName).readAllKeySet();
    }

    @Override
    public Future<Set<Object>> getKeysAsync(String mapName) {
        return getMap(mapName).readAllKeySetAsync();
    }

    @Override
    public Set<Object> getKeys(String mapName, String pattern) {
        return getMap(mapName).keySet(pattern);
    }

    @Override
    public boolean deleteAllKeys(String mapName) {
        return getMap(mapName).delete();
    }

    @Override
    public Future<Boolean> deleteAllKeysAsync(String mapName) {
        return getMap(mapName).deleteAsync();
    }

    @Override
    public Long deleteKeys(String mapName, Object... keys) {
        return getMap(mapName).fastRemove(keys);
    }

    @Override
    public Future<Long> deleteKeysAsync(String mapName, Object... keys) {
        return getMap(mapName).fastRemoveAsync(keys);
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
        return getMap(mapName).get(key);
    }

    @Override
    public Future<Object> getValueAsync(String mapName, Object key) {
        return getMap(mapName).getAsync(key);
    }

    @Override
    public Object getValues(String mapName, Set<Object> keys) {
        return getMap(mapName).getAll(keys);
    }

    @Override
    public Future<Map<Object, Object>> getValuesAsync(String mapName, Set<Object> keys) {
        return getMap(mapName).getAllAsync(keys);
    }

    private RMap<Object, Object> getMap(String mapName) {
        return redisson.getMap(mapName);
    }
}
