package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.monitor.exposed.service.MeterRegistryFactoryService;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreCacheService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;

@Component("servingStoreCacheService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class ServingStoreCacheServiceImpl extends MicroserviceRestApiProxy implements ServingStoreCacheService {

    private static final Logger log = LoggerFactory.getLogger(ServingStoreCacheServiceImpl.class);

    // FIXME remove temp capacity increase for matchapi
    private static final String MATCHAPI_SVC = "matchapi";
    private static final int SERVING_STORE_CACHE_LIMIT = 5;

    private ConcurrentHashMap<BusinessEntity, LocalCacheManager<String, List<ColumnMetadata>>> metadataCaches = new ConcurrentHashMap<>();

    private final DataCollectionProxy dataCollectionProxy;

    private final MetadataProxy metadataProxy;

    private ServingStoreCacheServiceImpl _service;

    private MeterRegistryFactoryService registryFactory;

    // FIXME remove temp capacity increase for matchapi
    @Value("${proxy.serving.store.matchapi.cache.limit}")
    private int servingStoreMatchapiLimit;

    @Inject
    protected ServingStoreCacheServiceImpl(DataCollectionProxy dataCollectionProxy, //
            MetadataProxy metadataProxy, ServingStoreCacheServiceImpl service,
            @Lazy MeterRegistryFactoryService registryFactory) {
        super("cdl");
        this.dataCollectionProxy = dataCollectionProxy;
        this.metadataProxy = metadataProxy;
        this._service = service;
        this.registryFactory = registryFactory;
    }

    @Override
    public List<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity) {
        String key = customerSpace + "|" + entity.name();
        return getOrCreateMetadataCache(entity).getWatcherCache().get(key);
    }

    @Cacheable(cacheNames = CacheName.Constants.ServingMetadataCacheName, key = "T(java.lang.String).format(\"%s|%s|decoratedmetadata\", #customerSpace, #entity)", unless="#result == null")
    public List<ColumnMetadata> getDecoratedMetadataFromDistributedCache(String customerSpace, BusinessEntity entity) {
        String key = customerSpace + "|" + entity.name();
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<ColumnMetadata> cms = getDecoratedMetadataFromApi(key);
            timer.setTimerMessage("Fetched " + CollectionUtils.size(cms) + " columns' metadata for " //
                    + entity + " in " + customerSpace);
            return cms;
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeCMCacheName, key = "T(java.lang.String).format(\"%s|%s|servingtable\", #customerSpace, #entity)", unless = "#result == null")
    public Set<String> getServingTableColumns(String customerSpace, BusinessEntity entity) {
        Set<String> result = null;
        if (entity != null) {
            String tableName = dataCollectionProxy.getTableName(customerSpace, entity.getServingStore());
            if (StringUtils.isBlank(tableName)) {
                log.info("Cannot find serving table for " + entity + " in " + customerSpace);
            } else {
                try (PerformanceTimer timer = new PerformanceTimer()) {
                    Set<String> attrSet = new HashSet<>();
                    List<ColumnMetadata> cms = metadataProxy.getTableColumns(customerSpace, tableName);
                    cms.forEach(cm -> attrSet.add(cm.getAttrName()));
                    timer.setTimerMessage(
                            "Fetched " + attrSet.size() + " attr names for " + entity + " in " + customerSpace);
                    if (CollectionUtils.isNotEmpty(attrSet)) {
                        result = attrSet;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void clearCache(String customerSpace, BusinessEntity entity) {
        String tenantId = ProxyUtils.shortenCustomerSpace(customerSpace);
        String keyPrefix = tenantId + "|" + entity.name();
        CacheService cacheService = CacheServiceBase.getCacheService();
        cacheService.refreshKeysByPattern(keyPrefix, CacheName.getCdlServingCacheGroup());
    }

    private List<ColumnMetadata> getDecoratedMetadataFromApi(String key) {
        String[] tokens = key.split("\\|");
        String customerSpace = tokens[0];
        BusinessEntity entity = BusinessEntity.valueOf(tokens[1]);
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/{entity}/decoratedmetadata", //
                shortenCustomerSpace(customerSpace), entity);
        List<?> list = get("serving store metadata", url, List.class);
        return JsonUtils.convertList(list, ColumnMetadata.class);
    }

    private List<ColumnMetadata> loadServingColumnMetadata(String cacheKey) {
        String[] tokens = cacheKey.split("\\|");
        String tenant = tokens[0];
        BusinessEntity entity = BusinessEntity.valueOf(tokens[1]);

        long estimatedSize = getOrCreateMetadataCache(entity).getWatcherCache().getEstimatedSize();
        Runtime rt = Runtime.getRuntime();
        long totalMb = rt.totalMemory() / 1024 / 1024;
        long freeMb = rt.freeMemory() / 1024 / 1024;
        log.info("Before inserting {}, approximately {} entries in the cache, total mem is {}, free mem is {}", //
                cacheKey, estimatedSize, totalMb, freeMb);
        return _service.getDecoratedMetadataFromDistributedCache(tenant, entity);
    }

    private LocalCacheManager<String, List<ColumnMetadata>> getOrCreateMetadataCache(BusinessEntity entity) {
        return metadataCaches.computeIfAbsent(entity, (e) -> {
            int cacheLimit = SERVING_STORE_CACHE_LIMIT;
            if (MATCHAPI_SVC.equals(BeanFactoryEnvironment.getService())) {
                log.info("Overriding serving store cache limit to {} for matchapi", servingStoreMatchapiLimit);
                cacheLimit = servingStoreMatchapiLimit;
            }
            log.info("Instantiating serving metadata local cache for entity {}, capacity = {}", entity.name(),
                    cacheLimit);
            return new LocalCacheManager<>(CacheName.ServingMetadataLocalCache, this::loadServingColumnMetadata,
                    cacheLimit, 5, (cache) -> { //
                        if (cache instanceof Cache) {
                            String cacheName = String.format("%s|%s", CacheName.ServingMetadataLocalCache,
                                    entity.name());
                            log.info("Start monitoring serving store metadata cache {}", cacheName);
                            Cache<?, ?> caffineCache = (Cache<?, ?>) cache;
                            CaffeineCacheMetrics.monitor( //
                                    registryFactory.getHostLevelRegistry(MetricDB.LDC_Match), //
                                    caffineCache, cacheName);
                        }
                    });
        });
    }
}
