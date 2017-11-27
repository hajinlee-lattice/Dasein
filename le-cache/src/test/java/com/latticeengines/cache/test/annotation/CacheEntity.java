package com.latticeengines.cache.test.annotation;

import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Component("cacheEntity")
public class CacheEntity {

    private int v = 0;

    @Cacheable(cacheNames = "Test")
    public int getValue(int k) {
        return this.v;
    }

    @CachePut(cacheNames = "Test")
    public void putValue(int v) {
        this.v = v;
    }
}
