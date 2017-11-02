package com.latticeengines.cache.exposed.cachemanager;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import com.latticeengines.cache.LocalCache;
import com.latticeengines.domain.exposed.cache.CacheNames;
import com.latticeengines.domain.exposed.cache.operation.CacheOperation;

public class LocalCacheManager<K, V> implements CacheManager {

    private LocalCache<K, V> cache;

    public LocalCacheManager(CacheNames cacheName, Function<K, V> load, int capacity) {
        cache = new LocalCache<>(cacheName, load, capacity);
        cache.setEvictKeyResolver((updateSignal, existingKeys) -> {
            return cache.getDefaultKeyResolver(updateSignal, existingKeys, CacheOperation.Evict);
        });
        cache.setRefreshKeyResolver((updateSignal, existingKeys) -> {
            return cache.getDefaultKeyResolver(updateSignal, existingKeys, CacheOperation.Put);
        });
    }

    @Override
    public Cache getCache(String name) {
        if (name.equals(cache.getName())) {
            return cache;
        }
        return null;
    }

    @Override
    public Collection<String> getCacheNames() {
        return Collections.singletonList(cache.getName());
    }

}
