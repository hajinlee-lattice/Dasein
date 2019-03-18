package com.latticeengines.proxy.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
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

import reactor.core.publisher.Mono;

@Component("ratingProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class RatingProxyImpl extends MicroserviceRestApiProxy implements RatingProxy {

    private final RatingProxyImpl _ratingProxy;

    @Inject
    public RatingProxyImpl(RatingProxyImpl ratingProxy) {
        super("objectapi/customerspaces");
        this._ratingProxy = ratingProxy;
    }

    public DataPage getData(String customerSpace, FrontEndQuery frontEndQuery) {
        return getData(shortenCustomerSpace(customerSpace), frontEndQuery, null);
    }

    public DataPage getData(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            DataPage dataPage = getDataNonBlocking(tenantId, frontEndQuery, version).block(Duration.ofHours(1));
            int count = dataPage == null ? 0 : dataPage.getData().size();
            String msg = "Fetched a page of " + count + " rows.";
            timer.setTimerMessage(msg);
            return dataPage;
        }
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
        Map<String, Long> map = _ratingProxy.getCoverageFromCache(shortenCustomerSpace(customerSpace), frontEndQuery);
        return JsonUtils.convertMap(map, String.class, Long.class);
    }

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|%s|rating_coverage\", #tenantId, #frontEndQuery)")
    public Map<String, Long> getCoverageFromCache(String tenantId, FrontEndQuery frontEndQuery) {
        return getCoverageFromApi(
                String.format("%s|%s", tenantId, JsonUtils.serialize(frontEndQuery)));
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
        return postMono("getCount", url, frontEndQuery, Long.class).block(Duration.ofHours(1));
    }

    private Mono<DataPage> getDataNonBlocking(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url;
        if (version != null) {
            url = constructUrl("/{customerSpace}/rating/data?version={version}", tenantId, version);
        } else {
            url = constructUrl("/{customerSpace}/rating/data", tenantId);
        }
        return postMonoKryo("getData", url, frontEndQuery, DataPage.class);
    }

    private Map<String, Long> getCoverageFromApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        return getCoverageMonoFromApi(tenantId, frontEndQuery).block(Duration.ofHours(1));
    }

    private Mono<Map<String, Long>> getCoverageMonoFromApi(String tenantId, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/rating/coverage", tenantId);
        return postMapMono("getRatingCoverage", url, frontEndQuery);
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
