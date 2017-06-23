package com.latticeengines.camille.exposed.watchers;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class WatcherCache<K, V> {

    private static Log log = LogFactory.getLog(WatcherCache.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private static final Random random = new Random(System.currentTimeMillis());

    private final Function<K, V> load;
    private final String watcherName;
    private final Object[] initKeys;
    private final int capacity;
    private Cache<K, V> cache;

    WatcherCache(String watcherName, Function<K, V> load, int capacity, Object... initKeys) {
        this.load = load;
        this.watcherName = watcherName;
        this.initKeys = initKeys;
        this.capacity = capacity;
        NodeWatcher.registerWatcher(watcherName);
        NodeWatcher.registerListener(watcherName, () -> {
            log.info("ZK watcher " + watcherName + " changed, updating caches ...");
            refresh();
        });
    }

    public static <K, V> Builder<K, V> builder(String watcherName) {
        return new Builder<>(watcherName);
    }

    public V get(K key) {
        if (cache == null) {
            initialize();
        }
        if (cache.getIfPresent(key) == null) {
            loadKey(key);
        }
        return cache.getIfPresent(key);
    }

    public void scheduleInit(long duration, TimeUnit timeUnit) {
        log.info("Scheduled to initialize the WatcherCache " + watcherName + " after "+ duration + " " + timeUnit);
        scheduler.schedule(this::initialize, duration, timeUnit);
    }

    @SuppressWarnings("unchecked")
    public void initialize() {
        if (cache == null) {
            long startTime = System.currentTimeMillis();
            log.info("Start initializing the WatcherCache " + watcherName + " ...");
            cache = CacheBuilder.newBuilder().maximumSize(capacity).build();
            if (initKeys != null) {
                Arrays.stream(initKeys).map(k -> (K) k).forEach(this::loadKey);
            }
            double duration = new Long(System.currentTimeMillis() - startTime).doubleValue() / 60.0;
            log.info(String.format("Finished initializing the WatcherCache %s after %.2f secs.", watcherName, duration));
        }
    }

    private void refresh() {
        if (cache != null) {
            long startTime = System.currentTimeMillis();
            log.info("Start refreshing the WatcherCache " + watcherName + " ...");
            cache.asMap().keySet().forEach(this::loadKey);
            double duration = new Long(System.currentTimeMillis() - startTime).doubleValue() / 60.0;
            log.info(String.format("Finished refreshing the WatcherCache %s after %.2f secs.", watcherName, duration));
        }
    }

    private void loadKey(K key) {
        try {
            try {
                // avoid request spike on cached resource
                Thread.sleep(random.nextInt(3000));
            } catch (InterruptedException e) {
                // ignore
            }
            V val = load.apply(key);
            if (val == null) {
                log.info("Got null value when loading the key " + key + ". Skip adding it to the cache.");
            } else {
                cache.put(key, val);
            }
        } catch (Exception e) {
            log.error("Failed to load cache using key " + key);
        }
    }

    public static class Builder<K, V> {
        private Function<K, V> load;
        private String watcherName;
        private K[] initKeys;
        private int capacity = 10;

        Builder(String watcherName) {
            this.watcherName = watcherName;
        }

        public Builder load(Function<K, V> load) {
            this.load = load;
            return this;
        }

        public Builder maximum(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder initKeys(K[] initKeys) {
            this.initKeys = initKeys;
            return this;
        }

        public WatcherCache<K, V> build() {
            return new WatcherCache<>(watcherName, load, capacity, initKeys);
        }

    }

}
