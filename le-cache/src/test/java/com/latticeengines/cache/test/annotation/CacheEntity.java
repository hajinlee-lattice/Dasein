package com.latticeengines.cache.test.annotation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Component("cacheEntity")
public class CacheEntity {

    private ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<>();

    @Cacheable(cacheNames = "Test", unless = "#result == null")
    public Integer getValue(Integer k) {
        return getValueBypassCache(k);
    }

    @CachePut(cacheNames = "Test", key = "#k", unless = "#result == null")
    public Integer putValue(Integer k, Integer v) {
        return putValueBypassCache(k, v);
    }

    @CacheEvict(cacheNames = "Test")
    public void clear(Integer k) {
        map.remove(k);
    }

    public Integer getValueBypassCache(Integer k) {
        return map.get(k);
    }

    public Integer putValueBypassCache(Integer k, Integer v) {
        map.put(k, v);
        return v;
    }
}
