package com.latticeengines.proxy.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.time.Duration;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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

    private final EntityProxyImpl _entityProxy;

    @Inject
    public EntityProxyImpl(EntityProxyImpl entityProxy) {
        super("objectapi/customerspaces");
        this._entityProxy = entityProxy;
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
        return _entityProxy.getDataFromCache(shortenCustomerSpace(customerSpace), frontEndQuery);
    }

    @Override
    public DataPage getProducts(String customerSpace) {
        FrontEndQuery query = getProductsQuery();
        return _entityProxy.getDataFromCache(shortenCustomerSpace(customerSpace), query);
    }

    @Override
    public DataPage getProductsFromObjectApi(String customerSpace, DataCollection.Version version) {
        FrontEndQuery query = getProductsQuery();
        return getDataFromObjectApi(shortenCustomerSpace(customerSpace), query);
    }

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|%s|entity_data\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #frontEndQuery)")
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

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|%s|entity_ratingcount\", #tenantId, #frontEndQuery)")
    public Map<String, Long> getRatingCountFromCache(String tenantId, RatingEngineFrontEndQuery frontEndQuery) {
        return getRatingCountFromObjectApi(String.format("%s|%s", tenantId, JsonUtils.serialize(frontEndQuery)));
    }

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|%s|entity_count\", #tenantId, #frontEndQuery)")
    public String getCountFromCache(String tenantId, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        return String.valueOf(getCountFromObjectApi(String.format("%s|%s", tenantId, frontEndQuery.toString())));
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

    private FrontEndQuery getProductsQuery() {
        FrontEndQuery query = new FrontEndQuery();
        query.setAccountRestriction(null);
        query.setContactRestriction(null);
        query.addLookups(BusinessEntity.Product, InterfaceName.ProductId.name());
        query.addLookups(BusinessEntity.Product, InterfaceName.ProductName.name());
        query.setMainEntity(BusinessEntity.Product);
        return query;
    }
}
