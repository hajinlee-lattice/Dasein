package com.latticeengines.proxy.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import reactor.core.publisher.Mono;

@Component("entityProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class EntityProxyImpl extends MicroserviceRestApiProxy implements EntityProxy {

    private static final Logger log = LoggerFactory.getLogger(EntityProxyImpl.class);

    private final CacheManager cacheManager;

    private final EntityProxyImpl _entityProxy;

    private LocalCacheManager<String, Long> countCache;

    private LocalCacheManager<String, DataPage> dataCache;

    private LocalCacheManager<String, Map<String, Long>> ratingCache;

    @Inject
    public EntityProxyImpl(CacheManager cacheManager, EntityProxyImpl entityProxy) {
        super("objectapi/customerspaces");
        this._entityProxy = entityProxy;
        this.cacheManager = cacheManager;
        countCache = new LocalCacheManager<>(CacheName.EntityCountCache, str -> {
            String[] tokens = str.split("\\|");
            return getCountFromObjectApi(String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 2000); //

        dataCache = new LocalCacheManager<>(CacheName.EntityDataCache, str -> {
            String[] tokens = str.split("\\|");
            return getDataBySerializedKeyFromObjectApi(
                    String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 200); //

        ratingCache = new LocalCacheManager<>(CacheName.EntityRatingCountCache, str -> {
            String[] tokens = str.split("\\|");
            return getRatingCountFromObjectApi(String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 2000); //
    }

    @PostConstruct
    public void addCacheManager() {
        if (cacheManager instanceof CompositeCacheManager) {
            log.info("adding local entity cache manager to composite cache manager");
            ((CompositeCacheManager) cacheManager).setCacheManagers(Arrays.asList(countCache, dataCache, ratingCache));
        }
    }

    @Override
    public Long getCount(String customerSpace, FrontEndQuery frontEndQuery) {
        RestrictionOptimizer.optimize(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        return Long.valueOf(_entityProxy.getCountFromCache(shortenCustomerSpace(customerSpace), frontEndQuery));
    }

    @Override
    public DataPage getData(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        return _entityProxy.getDataFromCache(customerSpace, frontEndQuery);
    }

    @Cacheable(cacheNames = CacheName.Constants.EntityDataCacheName, key = "T(java.lang.String).format(\"%s|%s|data\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #frontEndQuery)")
    public DataPage getDataFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        return getDataBySerializedKeyFromObjectApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), frontEndQuery.toString()));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, Long> getRatingCount(String customerSpace, RatingEngineFrontEndQuery frontEndQuery) {
        frontEndQuery = normalizeRatingCountQuery(frontEndQuery);
        Map map = _entityProxy.getRatingCountFromCache(shortenCustomerSpace(customerSpace), frontEndQuery);
        return JsonUtils.convertMap(map, String.class, Long.class);
    }

    /**
     * Do not directly call this method, unless you are sure you have standardize the customerSpace
     */
    @Cacheable(cacheNames = CacheName.Constants.EntityRatingCountCacheName, key = "T(java.lang.String).format(\"%s|%s|ratingcount\", #customerSpace, #frontEndQuery)")
    public Map<String, Long> getRatingCountFromCache(String customerSpace, RatingEngineFrontEndQuery frontEndQuery) {
        return getRatingCountFromObjectApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), JsonUtils.serialize(frontEndQuery)));
    }

    /**
     * Do not directly call this method, unless you are sure you have standardize the customerSpace
     */
    @Cacheable(cacheNames = CacheName.Constants.EntityCountCacheName, key = "T(java.lang.String).format(\"%s|%s|count\", #customerSpace, #frontEndQuery)")
    public String getCountFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        return String.valueOf(getCountFromObjectApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), frontEndQuery.toString())));
    }

    @Override
    public Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery) {
        return getCountFromObjectApi(tenantId, frontEndQuery, null);
    }

    @Override
    public DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery) {
        return getDataFromObjectApi(tenantId, frontEndQuery, null);
    }

    @Override
    public DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version) {
        return getDataFromObjectApi(tenantId, frontEndQuery, version, false);
    }

    @Override
    public DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version,
            boolean enforceTranslation) {
        String url = "/{customerSpace}/entity/data?enforceTranslation={enforceTranslation}";

        if (version != null) {
            url = constructUrl(url + "&version={version}", tenantId, enforceTranslation, version);
        } else {
            url = constructUrl(url, tenantId, enforceTranslation);
        }

        return postKryo("getData", url, frontEndQuery, DataPage.class);
    }

    private RatingEngineFrontEndQuery normalizeRatingCountQuery(RatingEngineFrontEndQuery frontEndQuery) {
        if (StringUtils.isBlank(frontEndQuery.getRatingEngineId())) {
            throw new UnsupportedOperationException("Rating count api must have rating engine id.");
        }
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        return frontEndQuery;
    }

    private Long getCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        return getCountFromObjectApi(tenantId, frontEndQuery);
    }

    public Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url;
        if (version != null) {
            url = constructUrl("/{customerSpace}/entity/count?version={version}", tenantId, version);
        } else {
            url = constructUrl("/{customerSpace}/entity/count", tenantId);
        }
        return post("getCount", url, frontEndQuery, Long.class);
    }

    private DataPage getDataBySerializedKeyFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        return getDataFromObjectApi(tenantId, frontEndQuery);
    }

    private Map<String, Long> getRatingCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        RatingEngineFrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery,
                RatingEngineFrontEndQuery.class);
        return getRatingCountMonoFromApi(tenantId, frontEndQuery).block(Duration.ofHours(1));
    }

    private Mono<Map<String, Long>> getRatingCountMonoFromApi(String tenantId,
            RatingEngineFrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/entity/ratingcount", tenantId);
        return postMapMono("getRatingCount", url, frontEndQuery);
    }

    private void optimizeRestrictions(FrontEndQuery frontEndQuery) {
        if (frontEndQuery.getAccountRestriction() != null) {
            Restriction restriction = frontEndQuery.getAccountRestriction().getRestriction();
            if (restriction != null) {
                frontEndQuery.getAccountRestriction().setRestriction(RestrictionOptimizer.optimize(restriction));
            }
        }
        if (frontEndQuery.getContactRestriction() != null) {
            Restriction restriction = frontEndQuery.getContactRestriction().getRestriction();
            if (restriction != null) {
                frontEndQuery.getContactRestriction().setRestriction(RestrictionOptimizer.optimize(restriction));
            }
        }
    }
}
