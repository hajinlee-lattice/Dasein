package com.latticeengines.metadata.entitymgr.impl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.service.SegmentationDataCollectionService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class DataCollectionCache {
    private static final Log log = LogFactory.getLog(DataCollectionCache.class);

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private SegmentationDataCollectionService segmentationDataCollectionService;

    @Autowired
    private TenantService tenantService;

    private LoadingCache<Pair<String, DataCollectionType>, DataCollection> dataCollectionCache = CacheBuilder
            .newBuilder().maximumSize(1000).expireAfterWrite(60, TimeUnit.MINUTES)
            .build(new CacheLoader<Pair<String, DataCollectionType>, DataCollection>() {
                @Override
                public DataCollection load(Pair<String, DataCollectionType> pair) throws Exception {
                    log.info(String.format("Loading data collection for customer %s type %s", pair.getLeft(),
                            pair.getRight()));
                    Tenant previous = MultiTenantContext.getTenant();
                    try {
                        Tenant tenant = tenantService.findByTenantId(pair.getLeft());
                        if (tenant != null) {
                            MultiTenantContext.setTenant(tenant);
                            DataCollection collection = dataCollectionEntityMgr.getDataCollection(pair.getRight());
                            dataCollectionEntityMgr.fillInTables(collection);
                            return collection;
                        }
                        log.warn(String.format("Could not find tenant %s", pair.getLeft()));
                        return null;
                    } finally {
                        MultiTenantContext.setTenant(previous);
                    }
                }
            });

    private ScheduledExecutorService cacheReloader = Executors.newSingleThreadScheduledExecutor();

    @PostConstruct
    public void setup() {
        cacheReloader.scheduleWithFixedDelay(() -> {
            for (Pair<String, DataCollectionType> pair : dataCollectionCache.asMap().keySet()) {
                dataCollectionCache.refresh(pair);
            }
        }, 0, 15, TimeUnit.MINUTES);
    }

    public DataCollection get(DataCollectionType type) {
        try {
            return dataCollectionCache.get(new ImmutablePair<>(MultiTenantContext.getTenant().getId(), type));
        } catch (ExecutionException e) {
            return null;
        }
    }

    public void invalidate(DataCollectionType type) {
        dataCollectionCache.refresh(new ImmutablePair<>(MultiTenantContext.getTenant().getId(), type));
    }
}
