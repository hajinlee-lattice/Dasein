package com.latticeengines.cache.exposed.cachemanager;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import com.latticeengines.cache.LocalCache;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cache.operation.CacheOperation;

public class LocalCacheManager<K, V> implements CacheManager {

    private LocalCache<K, V> cache;

    public LocalCacheManager(CacheName cacheName, Function<K, V> load, int capacity) {
        this(cacheName, load, capacity, 0);
    }

    public LocalCacheManager(CacheName cacheName, Function<K, V> load, int capacity, int waitBeforeRefreshInSec) {
        cache = new LocalCache<>(cacheName, load, capacity, waitBeforeRefreshInSec);
        cache.setEvictKeyResolver((updateSignal, existingKeys) -> //
        cache.getDefaultKeyResolver(updateSignal, existingKeys, CacheOperation.Evict));
        cache.setRefreshKeyResolver((updateSignal, existingKeys) -> //
        cache.getDefaultKeyResolver(updateSignal, existingKeys, CacheOperation.Put));
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

    public WatcherCache<K, V> getWatcherCache() {
        return cache.getWatcherCache();
    }

}
