package com.latticeengines.camille.exposed.watchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.latticeengines.camille.exposed.CamilleEnvironment;

public class WatcherCache<K, V> {

    private static Logger log = LoggerFactory.getLogger(WatcherCache.class);
    private static final double SEC_TO_MSEC = 1000.0;
    private static final Random random = new Random(System.currentTimeMillis());

    private final Function<K, V> load;
    private final String cacheName;
    private final String watcherName;
    private final Object[] initKeys;
    private final int capacity;
    private long expire;
    private TimeUnit expireUnit;
    private BiFunction<String, Set<K>, Collection<K>> refreshKeyResolver;
    private BiFunction<String, Set<K>, Collection<K>> evictKeyResolver;
    private Cache<K, V> cache;
    // To avoid congestion caused by simultaneously refresh
    private int waitBeforeRefreshInSec = 0;

    WatcherCache(String cacheName, String watcherName, Function<K, V> load, int capacity, int waitBeforeRefreshInSec,
            Object... initKeys) {
        this.load = load;
        this.cacheName = cacheName;
        this.watcherName = watcherName;
        this.initKeys = initKeys;
        this.capacity = capacity;
        this.refreshKeyResolver = (s, k) -> cache.asMap().keySet();
        this.evictKeyResolver = (s, k) -> Collections.emptyList();
        this.waitBeforeRefreshInSec = waitBeforeRefreshInSec;
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    public V get(K key) {
        if (cache == null) {
            initialize();
        }
        if (cache.getIfPresent(key) == null) {
            synchronized (this) {
                if (cache.getIfPresent(key) == null) {
                    loadKey(key);
                }
            }
        }
        return cache.getIfPresent(key);
    }

    public V getWithoutLoading(K key) {
        if (cache == null) {
            initialize();
        }
        return cache.getIfPresent(key);
    }

    public String getCacheName() {
        return cacheName;
    }

    public Object getNativeCache() {
        return cache;
    }

    public void setExpire(long expire, TimeUnit expireUnit) {
        this.expire = expire;
        this.expireUnit = expireUnit;
    }

    @SuppressWarnings("unchecked")
    public synchronized void initialize() {
        if (cache == null) {
            long startTime = System.currentTimeMillis();
            log.info("Start initializing the WatcherCache " + cacheName + " watching " + watcherName
                    + " ...");
            waitForCamille();
            NodeWatcher.registerWatcher(watcherName);
            NodeWatcher.registerListener(watcherName, () -> {
                log.info("ZK watcher " + watcherName + " changed, updating " + cacheName + " ...");
                refresh(NodeWatcher.getWatchedData(watcherName));
            });
            @SuppressWarnings("rawtypes")
            Caffeine caffeine = Caffeine.newBuilder().maximumSize(capacity);
            if (expireUnit != null) {
                caffeine.expireAfterWrite(expire, expireUnit);
            }
            cache = caffeine.build();
            if (initKeys != null) {
                log.info("Loading " + initKeys.length + " initial keys.");
                Arrays.stream(initKeys).map(k -> (K) k).forEach(this::loadKey);
            }
            double duration = new Long(System.currentTimeMillis() - startTime).doubleValue()
                    / SEC_TO_MSEC;
            log.info(String.format("Finished initializing the WatcherCache %s after %.3f secs.",
                    cacheName, duration));
        }
    }

    private void waitForCamille() {
        int retries = 0;
        while (!CamilleEnvironment.isStarted() && retries++ < 100) {
            try {
                log.info("Wait one sec for camille to start ...");
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void refresh(String watchedData) {
        if (cache != null) {
            log.info("Received a signal " + String.valueOf(watchedData));
            if (waitBeforeRefreshInSec != 0) {
                log.info(String.format(
                        "To avoid WatcherCache %s refresh congestion for signal %s, wait for %d seconds before start.",
                        cacheName, String.valueOf(watchedData), waitBeforeRefreshInSec));
                try {
                    Thread.sleep(waitBeforeRefreshInSec * 1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            log.info(String.format("Start refreshing WatcherCache %s for signal %s.", cacheName, watchedData));
            long startTime = System.currentTimeMillis();
            Collection<K> keysToEvict = new ArrayList<>(
                    evictKeyResolver.apply(watchedData, cache.asMap().keySet()));
            if (!keysToEvict.isEmpty()) {
                log.info("Going to evict " + keysToEvict.size() + " keys.");
                keysToEvict.forEach(cache::invalidate);
            }
            Collection<K> keysToRefresh = new ArrayList<>(
                    refreshKeyResolver.apply(watchedData, cache.asMap().keySet()));
            keysToRefresh.retainAll(cache.asMap().keySet());
            if (!keysToRefresh.isEmpty()) {
                log.info("Going to refresh " + keysToRefresh.size() + " keys.");
                cache.invalidateAll(keysToRefresh);
                keysToRefresh.forEach(this::loadKey);
            }
            double duration = new Long(System.currentTimeMillis() - startTime).doubleValue()
                    / SEC_TO_MSEC;
            log.info(String.format(
                    "Finished refreshing the WatcherCache %s for signal %s after %.3f secs.",
                    cacheName, watchedData, duration));
        }
    }

    public synchronized void put(K key, V value) {
        if (cache == null) {
            initialize();
        }
        cache.put(key, value);
    }

    private synchronized void loadKey(K key) {
        try {
            try {
                // avoid request spike on cached resource
                Thread.sleep(random.nextInt(3000));
            } catch (InterruptedException e) {
                log.warn("Thread sleep interrupted.", e);
            }
            if (load == null) {
                return;
            }
            V val = load.apply(key);
            if (val == null) {
                log.info("Got null value when loading the key " + key
                        + ". Skip adding it to the WatcherCache " + cacheName + ".");
            } else {
                cache.put(key, val);
                log.info(String.format("Loaded WatcherCache %s for key ", cacheName) + key);
            }
        } catch (Exception e) {
            log.error("Failed to load WatcherCache " + cacheName + " using key " + key, e);
        }
    }

    public void setRefreshKeyResolver(
            BiFunction<String, Set<K>, Collection<K>> refreshKeyResolver) {
        this.refreshKeyResolver = refreshKeyResolver;
    }

    public void setEvictKeyResolver(BiFunction<String, Set<K>, Collection<K>> evictKeyResolver) {
        this.evictKeyResolver = evictKeyResolver;
    }

    public static class Builder<K, V> {
        private Function<K, V> load;
        private String cacheName;
        private String watcherName;
        private Object[] initKeys;
        private int capacity = 10;
        private int waitBeforeRefreshInSec = 0;

        Builder() {
            this.watcherName = "Watcher-" + UUID.randomUUID().toString();
            this.cacheName = watcherName;
        }

        @SuppressWarnings("rawtypes")
        public Builder name(String cacheName) {
            this.cacheName = cacheName;
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder watch(String watcherName) {
            this.watcherName = watcherName;
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder load(Function<K, V> load) {
            this.load = load;
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder maximum(int capacity) {
            this.capacity = capacity;
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder initKeys(Object[] initKeys) {
            this.initKeys = initKeys;
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder waitBeforeRefreshInSec(int waitBeforeRefreshInSec) {
            this.waitBeforeRefreshInSec = waitBeforeRefreshInSec;
            return this;
        }

        public WatcherCache<K, V> build() {
            return new WatcherCache<>(cacheName, watcherName, load, capacity, waitBeforeRefreshInSec, initKeys);
        }

    }

}
