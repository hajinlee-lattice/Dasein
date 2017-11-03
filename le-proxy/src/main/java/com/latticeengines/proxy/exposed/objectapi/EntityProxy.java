package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Arrays;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cache.CacheNames;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("entityProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class EntityProxy extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(EntityProxy.class);

    @Autowired
    private CacheManager cacheManager;

    private LocalCacheManager<String, Long> countCache;

    private LocalCacheManager<String, DataPage> dataCache;

    private LocalCacheManager<String, Map<String, Long>> ratingCache;

    public EntityProxy() {
        super("objectapi/customerspaces");
        countCache = new LocalCacheManager<>(CacheNames.EntityCountCache, o -> {
            String str = (String) o;
            String[] tokens = str.split("\\|");
            return getCountFromObjectApi(String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 2000); //

        dataCache = new LocalCacheManager<>(CacheNames.EntityDataCache, o -> {
            String str = (String) o;
            String[] tokens = str.split("\\|");
            return getDataFromObjectApi(String.format("%s|%s", shortenCustomerSpace(tokens[0]), tokens[1]));
        }, 200); //

        ratingCache = new LocalCacheManager<>(CacheNames.EntityRatingCountCache, o -> {
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

    @Cacheable(cacheNames = "EntityCountCache", key = "T(java.lang.String).format(\"%s|%s|count\", #customerSpace, #frontEndQuery)", sync = true)
    public Long getCount(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        return getCountFromCache(customerSpace, frontEndQuery);
    }

    @Cacheable(cacheNames = "EntityDataCache", key = "T(java.lang.String).format(\"%s|%s|data\", #customerSpace, #frontEndQuery)", sync = true)
    public DataPage getData(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        return getDataFromCache(customerSpace, frontEndQuery);
    }

    @Cacheable(cacheNames = "EntityRatingCountCache", key = "T(java.lang.String).format(\"%s|%s|ratingcount\", #customerSpace, #frontEndQuery)", sync = true)
    public Map<String, Long> getRatingCount(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        return getRatingCountFromCache(customerSpace, frontEndQuery);
    }

    // @Cacheable(cacheNames = "EntityCountCache", key =
    // "T(java.lang.String).format(\"%s|%s|count\", #customerSpace,
    // #frontEndQuery)", sync = true)
    public Long getCountFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        Long count = getCountFromObjectApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), frontEndQuery.toString()));
        if (count == null) {
            throw new LedpException(LedpCode.LEDP_18158);
        }
        return count;
    }

    // @Cacheable(cacheNames = "EntityDataCache", key =
    // "T(java.lang.String).format(\"%s|%s|data\", #customerSpace,
    // #frontEndQuery)", sync = true)
    public DataPage getDataFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        DataPage dataPage = getDataFromObjectApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), frontEndQuery.toString()));
        if (dataPage == null || dataPage.getData() == null) {
            throw new LedpException(LedpCode.LEDP_18158);
        }

        return dataPage;
    }

    // @Cacheable(cacheNames = "EntityRatingCountCache", key =
    // "T(java.lang.String).format(\"%s|%s|ratingcount\", #customerSpace,
    // #frontEndQuery)", sync = true)
    public Map<String, Long> getRatingCountFromCache(String customerSpace, FrontEndQuery frontEndQuery) {
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null);
        Map<String, Long> ratingCountInfo = getRatingCountFromObjectApi(
                String.format("%s|%s", shortenCustomerSpace(customerSpace), JsonUtils.serialize(frontEndQuery)));
        if (MapUtils.isEmpty(ratingCountInfo)) {
            throw new LedpException(LedpCode.LEDP_18158);
        }

        return ratingCountInfo;
    }

    public Long getCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/entity/count", tenantId);
        return post("getCount", url, frontEndQuery, Long.class);
    }

    private DataPage getDataFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/entity/data", tenantId);
        return post("getData", url, frontEndQuery, DataPage.class);
    }

    @SuppressWarnings({ "rawtypes" })
    private Map<String, Long> getRatingCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/entity/ratingcount", tenantId);
        Map map = post("getRatingCount", url, frontEndQuery, Map.class);
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
