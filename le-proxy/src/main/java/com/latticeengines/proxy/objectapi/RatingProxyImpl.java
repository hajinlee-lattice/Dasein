package com.latticeengines.proxy.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

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
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;

@Component("ratingProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class RatingProxyImpl extends MicroserviceRestApiProxy implements RatingProxy {

    private static final Logger log = LoggerFactory
            .getLogger(com.latticeengines.proxy.exposed.objectapi.RatingProxy.class);

    private final CacheManager cacheManager;

    private final RatingProxyImpl _ratingProxy;

    private LocalCacheManager<String, Map<String, Long>> coverageCache;

    @Inject
    public RatingProxyImpl(CacheManager cacheManager, RatingProxyImpl ratingProxy) {
        super("objectapi/customerspaces");
        this.cacheManager = cacheManager;
        this._ratingProxy = ratingProxy;
        coverageCache = new LocalCacheManager<>(CacheName.RatingCoverageCache, str -> {
            String[] tokens = str.split("\\|");
            return getCoverageFromApi(String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 2000); //
    }

    @PostConstruct
    public void postConstruct() {
        if (cacheManager instanceof CompositeCacheManager) {
            log.info("adding local entity cache manager to composite cache manager");
            ((CompositeCacheManager) cacheManager).setCacheManagers(Collections.singletonList(coverageCache));
        }
    }

    public DataPage getData(String customerSpace, FrontEndQuery frontEndQuery) {
        return getDataFromObjectApi(shortenCustomerSpace(customerSpace), frontEndQuery, null);
    }

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

        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        Map map = _ratingProxy.getCoverageFromCache(customerSpace, frontEndQuery);
        return JsonUtils.convertMap(map, String.class, Long.class);
    }

    @Cacheable(cacheNames = CacheName.Constants.RatingCoverageCacheName, key = "T(java.lang.String).format(\"%s|%s|coverage\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #frontEndQuery)", sync = true)
    public Map<String, Long> getCoverageFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        return getCoverageFromApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), JsonUtils.serialize(frontEndQuery)));
    }

    private RatingModel normalizeRatingModel(RatingModel ratingModel) {
        ratingModel.setId("RatingEngine");
        ratingModel.setPid(null);
        ratingModel.setCreated(null);
        ratingModel.setIteration(-1);
        ratingModel.setRatingEngine(null);
        return ratingModel;
    }

    public Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url;
        if (version != null) {
            url = constructUrl("/{customerSpace}/rating/count?version={version}", tenantId, version);
        } else {
            url = constructUrl("/{customerSpace}/rating/count", tenantId);
        }
        return post("getCount", url, frontEndQuery, Long.class);
    }

    public DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url;
        if (version != null) {
            url = constructUrl("/{customerSpace}/rating/data?version={version}", tenantId, version);
        } else {
            url = constructUrl("/{customerSpace}/rating/data", tenantId);
        }
        return postKryo("getData", url, frontEndQuery, DataPage.class);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Map<String, Long> getCoverageFromApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/rating/coverage", tenantId);
        return post("getRatingCoverage", url, frontEndQuery, Map.class);
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
