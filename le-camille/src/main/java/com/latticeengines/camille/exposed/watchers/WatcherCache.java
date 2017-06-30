package com.latticeengines.camille.exposed.watchers;

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher;

public class WatcherCache<K, V> {

    private static Log log = LogFactory.getLog(WatcherCache.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private static final Random random = new Random(System.currentTimeMillis());

    private final Function<K, V> load;
    private final String cacheName;
    private final String watcherName;
    private final Object[] initKeys;
    private final Callable<Object[]> initKeysCallable;
    private final int capacity;
    private Cache<K, V> cache;

    WatcherCache(String cacheName, String watcherName, Function<K, V> load, int capacity, Object... initKeys) {
        this.load = load;
        this.cacheName = cacheName;
        this.watcherName = watcherName;
        this.initKeys = initKeys;
        this.capacity = capacity;
        this.initKeysCallable = null;
    }

    WatcherCache(String cacheName, String watcherName, Function<K, V> load, int capacity, Callable<Object[]> initKeysCallable) {
        this.load = load;
        this.cacheName = cacheName;
        this.watcherName = watcherName;
        this.initKeys = null;
        this.initKeysCallable = initKeysCallable;
        this.capacity = capacity;
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
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
        log.info("Scheduled to initialize the WatcherCache " + cacheName + " watching " + watcherName + " after "
                + duration + " " + timeUnit);
        scheduler.schedule(this::initialize, duration, timeUnit);
    }

    @SuppressWarnings("unchecked")
    public synchronized void initialize() {
        if (cache == null) {
            long startTime = System.currentTimeMillis();
            log.info("Start initializing the WatcherCache " + cacheName + " watching " + watcherName + " ...");
            waitForCamille();
            NodeWatcher.registerWatcher(watcherName);
            NodeWatcher.registerListener(watcherName, () -> {
                log.info("ZK watcher " + watcherName + " changed, updating " + cacheName + " ...");
                refresh();
            });
            cache = CacheBuilder.newBuilder().maximumSize(capacity).build();
            if (initKeysCallable != null) {
                try {
                    Object[] keys = initKeysCallable.call();
                    Arrays.stream(keys).map(k -> (K) k).forEach(this::loadKey);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to load initial keys via callable.", e);
                }
            } else if (initKeys != null) {
                Arrays.stream(initKeys).map(k -> (K) k).forEach(this::loadKey);
            }
            double duration = new Long(System.currentTimeMillis() - startTime).doubleValue() / 1000.0;
            log.info(
                    String.format("Finished initializing the WatcherCache %s after %.3f secs.", cacheName, duration));
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

    private void refresh() {
        if (cache != null) {
            long startTime = System.currentTimeMillis();
            log.info("Start refreshing the WatcherCache " + cacheName + " watching " + watcherName + " ...");
            cache.asMap().keySet().forEach(this::loadKey);
            double duration = new Long(System.currentTimeMillis() - startTime).doubleValue() / 1000.0;
            log.info(String.format("Finished refreshing the WatcherCache %s after %.3f secs.", cacheName, duration));
        }
    }

    private synchronized void loadKey(K key) {
        try {
            try {
                // avoid request spike on cached resource
                Thread.sleep(random.nextInt(3000));
            } catch (InterruptedException e) {
                // ignore
            }
            V val = load.apply(key);
            if (val == null) {
                log.info("Got null value when loading the key " + key + ". Skip adding it to the WatcherCache "
                        + cacheName + ".");
            } else {
                cache.put(key, val);
            }
        } catch (Exception e) {
            log.error("Failed to load WatcherCache " + cacheName + " using key " + key);
        }
    }

    public static class Builder<K, V> {
        private Function<K, V> load;
        private Callable<Object[]> initKeysCallable;
        private String cacheName;
        private String watcherName;
        private Object[] initKeys;
        private int capacity = 10;

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
        public Builder watch(CamilleWatcher watcher) {
            this.watcherName = watcher.name();
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
        public Builder initKeys(Callable<Object[]> initKeysCallable) {
            this.initKeysCallable = initKeysCallable;
            return this;
        }

        public WatcherCache<K, V> build() {
            if (initKeysCallable != null) {
                return new WatcherCache<>(cacheName, watcherName, load, capacity, initKeysCallable);
            } else {
                return new WatcherCache<>(cacheName, watcherName, load, capacity, initKeys);
            }
        }

    }

}
