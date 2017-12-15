package com.latticeengines.proxy.exposed.objectapi;

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
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("ratingProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class RatingProxy extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(RatingProxy.class);

    private final CacheManager cacheManager;

    private LocalCacheManager<String, DataPage> dataCache;

    private LocalCacheManager<String, Map<String, Long>> coverageCache;

    @Inject
    public RatingProxy(CacheManager cacheManager) {
        super("objectapi/customerspaces");
        this.cacheManager = cacheManager;

        dataCache = new LocalCacheManager<>(CacheName.RatingDataCache, o -> {
            String str = (String) o;
            String[] tokens = str.split("\\|");
            return getDataFromObjectApi(String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 200); //

        coverageCache = new LocalCacheManager<>(CacheName.RatingCoverageCache, o -> {
            String str = (String) o;
            String[] tokens = str.split("\\|");
            return getCoverageFromApi(String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 2000); //
    }

    @PostConstruct
    public void addCacheManager() {
        if (cacheManager instanceof CompositeCacheManager) {
            log.info("adding local entity cache manager to composite cache manager");
            ((CompositeCacheManager) cacheManager).setCacheManagers(Arrays.asList(dataCache, coverageCache));
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.RatingDataCacheName, key = "T(java.lang.String).format(\"%s|%s|data\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #frontEndQuery)", sync = true)
    public DataPage getData(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        DataPage page = getDataFromCache(customerSpace, frontEndQuery);
        return page;
    }

    @Cacheable(cacheNames = CacheName.Constants.RatingCoverageCacheName, key = "T(java.lang.String).format(\"%s|%s|ratingcount\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #frontEndQuery)", sync = true)
    public Map<String, Long> getCoverage(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        if (CollectionUtils.isEmpty(frontEndQuery.getRatingModels()) || frontEndQuery.getRatingModels().size() != 1) {
            throw new UnsupportedOperationException("Rating count api only works with single rating model.");
        }
        // normalize rating model to increase cache hit
        RatingModel ratingModel = normalizeRatingModel(frontEndQuery.getRatingModels().get(0));
        frontEndQuery.setRatingModels(Collections.singletonList(ratingModel));
        return getCoverageFromCache(customerSpace, frontEndQuery);
    }

    private RatingModel normalizeRatingModel(RatingModel ratingModel) {
        ratingModel.setId("RatingEngine");
        ratingModel.setPid(null);
        ratingModel.setCreated(null);
        ratingModel.setIteration(-1);
        ratingModel.setRatingEngine(null);
        ratingModel.setParentEngine(null);
        return ratingModel;
    }

    private DataPage getDataFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        return getDataFromObjectApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), frontEndQuery.toString()));
    }

    private Map<String, Long> getCoverageFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        return getCoverageFromApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), JsonUtils.serialize(frontEndQuery)));
    }

    private DataPage getDataFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        return getDataFromObjectApi(tenantId, frontEndQuery);
    }

    public DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/entity/data", tenantId);
        return postWithRetries("getData", url, frontEndQuery, DataPage.class);
    }

    @SuppressWarnings({ "rawtypes" })
    private Map<String, Long> getCoverageFromApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/entity/ratingcount", tenantId);
        Map map = postWithRetries("getRatingCount", url, frontEndQuery, Map.class);
        if (map == null) {
            return null;
        } else {
            return JsonUtils.convertMap(map, String.class, Long.class);
        }
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
