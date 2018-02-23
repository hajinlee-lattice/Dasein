package com.latticeengines.proxy.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

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
        countCache = new LocalCacheManager<>(CacheName.EntityCountCache, o -> {
            String str = (String) o;
            String[] tokens = str.split("\\|");
            return getCountFromObjectApi(String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 2000); //

        dataCache = new LocalCacheManager<>(CacheName.EntityDataCache, o -> {
            String str = (String) o;
            String[] tokens = str.split("\\|");
            return getDataFromObjectApi(String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 200); //

        ratingCache = new LocalCacheManager<>(CacheName.EntityRatingCountCache, o -> {
            String str = (String) o;
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
        return Long.valueOf(_entityProxy.getCountFromCache(customerSpace, frontEndQuery));
    }

    @Override
    public DataPage getData(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        return getDataFromCache(customerSpace, frontEndQuery);
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.EntityDataCacheName, key = "T(java.lang.String).format(\"%s|%s|data\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #frontEndQuery)", sync = true)
    public DataPage getDataFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        return getDataFromObjectApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), frontEndQuery.toString()));
    }

    @Override
    public Map<String, Long> getRatingCount(String customerSpace, FrontEndQuery frontEndQuery) {
        frontEndQuery = getRatingCountQuery(frontEndQuery);
        return getRatingCountFromCache(customerSpace, frontEndQuery);
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.EntityRatingCountCacheName, key = "T(java.lang.String).format(\"%s|%s|ratingcount\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #frontEndQuery)", sync = true)
    public Map<String, Long> getRatingCountFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        return getRatingCountFromObjectApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), JsonUtils.serialize(frontEndQuery)));
    }

    @Cacheable(cacheNames = CacheName.Constants.EntityCountCacheName, key = "T(java.lang.String).format(\"%s|%s|count\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #frontEndQuery)", sync = true)
    public String getCountFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        return String.valueOf(getCountFromObjectApi(String.format("%s|%s", shortenCustomerSpace(customerSpace), frontEndQuery.toString())));
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
        String url;
        if (version != null) {
            url = constructUrl("/{customerSpace}/entity/data?version={version}", tenantId, version);
        } else {
            url = constructUrl("/{customerSpace}/entity/data", tenantId);
        }
        return post("getData", url, frontEndQuery, DataPage.class);
    }

    private FrontEndQuery getRatingCountQuery(FrontEndQuery frontEndQuery) {
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        if (CollectionUtils.isEmpty(frontEndQuery.getRatingModels()) || frontEndQuery.getRatingModels().size() != 1) {
            throw new UnsupportedOperationException("Rating count api only works with single rating model.");
        }

        // normalize rating model to increase cache hit
        RatingModel ratingModel = normalizeRatingModel(frontEndQuery.getRatingModels().get(0));
        frontEndQuery.setRatingModels(Collections.singletonList(ratingModel));

        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        return frontEndQuery;
    }

    private RatingModel normalizeRatingModel(RatingModel ratingModel) {
        ratingModel.setId("RatingEngine");
        ratingModel.setPid(null);
        ratingModel.setCreated(null);
        ratingModel.setIteration(-1);
        ratingModel.setRatingEngine(null);
        return ratingModel;
    }

    private Long getCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        return getCountFromObjectApi(tenantId, frontEndQuery);
    }

    private Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url;
        if (version != null) {
            url = constructUrl("/{customerSpace}/entity/count?version={version}", tenantId, version);
        } else {
            url = constructUrl("/{customerSpace}/entity/count", tenantId);
        }
        return post("getCount", url, frontEndQuery, Long.class);
    }

    private DataPage getDataFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/entity/data", tenantId);
        return post("getData", url, frontEndQuery, DataPage.class);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Map<String, Long> getRatingCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/entity/ratingcount", tenantId);
        return post("getRatingCount", url, frontEndQuery, Map.class);
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
