package com.latticeengines.cache.exposed.service;

public interface CacheService {

    void dropKeysByPattern(String cacheName, String pattern);

}
