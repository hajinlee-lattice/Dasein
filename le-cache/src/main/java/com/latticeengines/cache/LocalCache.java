package com.latticeengines.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cache.operation.CacheOperation;
import com.latticeengines.domain.exposed.util.CacheUtils;

public class LocalCache<K, V> implements Cache {

    private static final Logger log = LoggerFactory.getLogger(LocalCache.class);

    private WatcherCache<K, V> cache;

    public LocalCache(CacheName cacheName, Function<K, V> load, int capacity) {
        this(cacheName, load, capacity, 0);
    }

    @SuppressWarnings("unchecked")
    public LocalCache(CacheName cacheName, Function<K, V> load, int capacity, int waitBeforeRefreshInSec) {
        cache = WatcherCache.builder() //
                .name(cacheName.name()) //
                .watch(cacheName.name()) //
                .maximum(capacity) //
                .load(load) //
                .waitBeforeRefreshInSec(waitBeforeRefreshInSec) //
                .build();
    }

    @Override
    public String getName() {
        return cache.getCacheName();
    }

    @Override
    public Object getNativeCache() {
        return cache.getNativeCache();
    }

    public WatcherCache<K, V> getWatcherCache() {
        return cache;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ValueWrapper get(Object key) {
        Object value = cache.getWithoutLoading((K) key);
        if (value == null) {
            return null;
        }
        return new SimpleValueWrapper(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(Object key, Class<T> type) {
        Object value = cache.getWithoutLoading((K) key);
        if (value == null) {
            return null;
        }
        return type.cast(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        Object value = cache.getWithoutLoading((K) key);
        if (value == null) {
            synchronized (key) {
                value = cache.getWithoutLoading((K) key);
                if (value == null) {
                    try {
                        value = valueLoader.call();
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("can't load value for key %s", key), e);
                    }
                }
                put(key, value);
            }
        }
        return (T) value;
    }

    @Override
    public void put(Object key, Object value) {
        // cache.put((K) key, (V) value);
        NodeWatcher.notifyCacheWatchersAsync(cache.getCacheName(),
                CacheUtils.getKeyOperation(CacheOperation.Put, key.toString()));
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            log.warn("Thread sleep interrupted", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        Object prevValue = cache.getWithoutLoading((K) key);
        if (prevValue == null) {
            synchronized (key) {
                prevValue = cache.getWithoutLoading((K) key);
                if (prevValue == null) {
                    cache.put((K) key, (V) value);
                } else {
                    return new SimpleValueWrapper(prevValue);
                }
            }
        }
        return new SimpleValueWrapper(value);
    }

    @Override
    public void evict(Object key) {
        NodeWatcher.notifyCacheWatchersAsync(cache.getCacheName(),
                CacheUtils.getKeyOperation(CacheOperation.Evict, key.toString()));
    }

    @Override
    public void clear() {
        NodeWatcher.notifyCacheWatchersAsync(cache.getCacheName(),
                CacheUtils.getAllOperation(CacheOperation.Evict, ""));
    }

    public List<K> getDefaultKeyResolver(String updateSignal, Set<K> existingKeys, CacheOperation expectedOp) {
        String[] tokens = updateSignal.split("\\|");
        String timestamp = tokens[0];
        String opStr = tokens[1];
        String mode = tokens[2];
        CacheOperation op = CacheOperation.valueOf(opStr);
        if (op != expectedOp) {
            return Collections.emptyList();
        }
        final String keyPattern = updateSignal.replace(String.format("%s|%s|%s|", timestamp, opStr, mode), "");
        List<K> keysToReturn = new ArrayList<>();
        existingKeys.forEach(key -> {
            if (mode.equals("key")) {
                if (key.toString().equals(keyPattern)) {
                    keysToReturn.add(key);
                }
            } else if (mode.equals("all")) {
                if (key.toString().startsWith(keyPattern)) {
                    keysToReturn.add(key);
                }
            }
        });
        log.info(String.format("Local cache %s received %s %s signal for %s, resolved to %d keys.", getName(), opStr,
                mode, keyPattern, keysToReturn.size()));
        return keysToReturn;
    }

    public void setEvictKeyResolver(BiFunction<String, Set<K>, Collection<K>> evictKeyResolver) {
        cache.setEvictKeyResolver(evictKeyResolver);
    }

    public void setRefreshKeyResolver(BiFunction<String, Set<K>, Collection<K>> refreshKeyResolver) {
        cache.setRefreshKeyResolver(refreshKeyResolver);
    }
}
