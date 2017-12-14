package com.latticeengines.cache.service.impl;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cache.CacheType;
import com.latticeengines.domain.exposed.cache.operation.CacheOperation;
import com.latticeengines.domain.exposed.util.CacheUtils;

@Component("localCacheService")
public class LocalCacheService extends CacheServiceBase {

    private static final Logger log = LoggerFactory.getLogger(LocalCacheService.class);

    protected LocalCacheService() {
        super(CacheType.Local);
    }

    @Override
    public void refreshKeysByPattern(String pattern, CacheName... cacheNames) {
        Arrays.stream(cacheNames).forEach(cache -> {
            log.info("Notifying the cache " + cache + " to refresh key pattern " + pattern);
            NodeWatcher.notifyCacheWatchersAsync(cache.name(), CacheUtils.getAllOperation(CacheOperation.Put, pattern));
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                log.warn("Thread sleep interrupted", e);
            }
        });
    }

}
