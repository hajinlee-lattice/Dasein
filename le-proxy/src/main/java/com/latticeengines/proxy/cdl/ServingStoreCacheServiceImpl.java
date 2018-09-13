package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyUtils;
import com.latticeengines.proxy.exposed.cdl.ServingStoreCacheService;

@Component("servingStoreCacheService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class ServingStoreCacheServiceImpl extends MicroserviceRestApiProxy implements ServingStoreCacheService {

    private static final Logger log = LoggerFactory.getLogger(ServingStoreCacheServiceImpl.class);

    private LocalCacheManager<String, List<ColumnMetadata>> metadataCache;

    private final CacheManager cacheManager;

    @Inject
    protected ServingStoreCacheServiceImpl(CacheManager cacheManager) {
        super("cdl");
        this.cacheManager = cacheManager;
        metadataCache = new LocalCacheManager<>(CacheName.ServingMetadataCache, this::getDecoratedMetadataFromApi, 200);
    }

    @PostConstruct
    public void addCacheManager() {
        if (cacheManager instanceof CompositeCacheManager) {
            log.info("adding local entity cache manager to composite cache manager");
            ((CompositeCacheManager) cacheManager).setCacheManagers(Collections.singletonList(metadataCache));
        }
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.ServingMetadataCacheName, key = "T(java.lang.String).format(\"%s|%s|decoratedmetadata\", #customerSpace, #entity)", unless="#result == null")
    public List<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity) {
        String key = customerSpace + "|" + entity.name();
        return getDecoratedMetadataFromApi(key);
    }

    @Override
    public void clearCache(String customerSpace, BusinessEntity entity) {
        String tenantId = ProxyUtils.shortenCustomerSpace(customerSpace);
        String keyPrefix = tenantId + "|" + entity.name();
        CacheService cacheService = CacheServiceBase.getCacheService();
        cacheService.refreshKeysByPattern(keyPrefix, CacheName.ServingMetadataCache);
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

}
